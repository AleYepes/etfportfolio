"""
One-time migration script:
Converts bronze.snapshots from an append-only log to a state-transition changelog.
Collapses contiguous identical (product_id, url_prefix, hash) rows into single
records with created_at = MIN(fetched_at) and last_checked_at = MAX(fetched_at).
"""

import sys
from pathlib import Path

# Ensure repository root is on sys.path when executed directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb

from etfportfolio.core.config import settings
from etfportfolio.core.db import apply_schema


def migrate(db_path: str | None = None) -> None:
    target_path = db_path or settings.db_path
    conn = duckdb.connect(target_path)
    try:
        cols = [col[1] for col in conn.execute("PRAGMA table_info('bronze.snapshots')").fetchall()]
        if "last_checked_at" in cols:
            print("bronze.snapshots already contains last_checked_at. No migration needed.")
            apply_schema(conn)
            return

        print("Migrating bronze.snapshots to state-transition changelog...")
        conn.execute("BEGIN TRANSACTION")

        # 1. Collapse contiguous blocks of identical hashes using window functions
        conn.execute("""
            CREATE TEMP TABLE snapshots_collapsed AS
            WITH marked AS (
                SELECT
                    snapshot_id,
                    hash,
                    product_id,
                    url_prefix,
                    url_slug,
                    fetched_at,
                    CASE
                        WHEN LAG(hash) OVER (PARTITION BY product_id, url_prefix ORDER BY snapshot_id) = hash THEN 0
                        ELSE 1
                    END AS is_new_block
                FROM bronze.snapshots
            ),
            grouped AS (
                SELECT
                    *,
                    SUM(is_new_block) OVER (PARTITION BY product_id, url_prefix ORDER BY snapshot_id) AS block_id
                FROM marked
            )
            SELECT
                MIN(snapshot_id) AS snapshot_id,
                hash,
                product_id,
                url_prefix,
                FIRST(url_slug) AS url_slug,
                MIN(fetched_at) AS created_at,
                MAX(fetched_at) AS last_checked_at
            FROM grouped
            GROUP BY product_id, url_prefix, block_id, hash
            ORDER BY snapshot_id;
        """)

        old_row = conn.execute("SELECT COUNT(*) FROM bronze.snapshots").fetchone()
        old_count = old_row[0] if old_row else 0
        new_row = conn.execute("SELECT COUNT(*) FROM snapshots_collapsed").fetchone()
        new_count = new_row[0] if new_row else 0

        # 2. Swap table into place
        conn.execute("DROP TABLE bronze.snapshots")
        conn.execute("""
            CREATE TABLE bronze.snapshots (
                snapshot_id     INTEGER PRIMARY KEY,
                hash            UBIGINT NOT NULL,
                product_id      INTEGER NOT NULL,
                url_prefix      VARCHAR NOT NULL,
                url_slug        VARCHAR,
                created_at      TIMESTAMP NOT NULL,
                last_checked_at TIMESTAMP NOT NULL
            )
        """)
        conn.execute("""
            INSERT INTO bronze.snapshots
            SELECT snapshot_id, hash, product_id, url_prefix, url_slug, created_at, last_checked_at
            FROM snapshots_collapsed
        """)
        conn.execute("DROP TABLE snapshots_collapsed")

        # 3. Synchronize sequence with max snapshot_id
        max_row = conn.execute("SELECT COALESCE(MAX(snapshot_id), 0) + 1 FROM bronze.snapshots").fetchone()
        max_id = max_row[0] if max_row else 1
        conn.execute("DROP SEQUENCE IF EXISTS bronze.snapshots_id_seq")
        conn.execute(f"CREATE SEQUENCE bronze.snapshots_id_seq START {max_id}")
        conn.execute(
            "ALTER TABLE bronze.snapshots ALTER COLUMN snapshot_id SET DEFAULT nextval('bronze.snapshots_id_seq')"
        )

        # 4. Re-apply schema to create all new silver tables
        apply_schema(conn)

        conn.execute("COMMIT")
        print(f"Migration complete: {old_count} raw snapshot rows collapsed into {new_count} changelog rows.")
    except Exception as e:
        conn.execute("ROLLBACK")
        print(f"Migration failed, rolled back: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    migrate()
