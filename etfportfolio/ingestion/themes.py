import logging
from datetime import UTC, datetime
from typing import Any

import duckdb
import httpx

from etfportfolio.core.config import settings
from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.core.logging import console
from etfportfolio.ingestion import session
from etfportfolio.ingestion.utils import is_fresh

logger = logging.getLogger(__name__)


def _check_themes_freshness(conn: duckdb.DuckDBPyConnection) -> datetime | None:
    row = conn.execute("SELECT MAX(last_checked_at) FROM bronze.themes").fetchone()
    return row[0] if row else None


def _count_themes(conn: duckdb.DuckDBPyConnection) -> tuple[int, int]:
    row_p = conn.execute("SELECT COUNT(*) FROM bronze.themes WHERE parent_id IS NULL").fetchone()
    row_n = conn.execute("SELECT COUNT(*) FROM bronze.themes WHERE parent_id IS NOT NULL").fetchone()
    parents = int(row_p[0]) if row_p else 0
    nodes = int(row_n[0]) if row_n else 0
    return parents, nodes


def upsert_themes(conn: duckdb.DuckDBPyConnection, payload: dict[str, Any]) -> tuple[int, int]:
    """Upserts parents first, then nodes into bronze.themes.

    Returns (parents_count, nodes_count).
    """
    parents = payload.get("parents", [])
    nodes = payload.get("nodes", [])

    existing_ids = {row[0] for row in conn.execute("SELECT theme_id FROM bronze.themes").fetchall()}

    insert_query = """
    INSERT INTO bronze.themes (theme_id, num_id, name, parent_id, created_at, updated_at)
    VALUES ($1, $2, $3, $4, (now() AT TIME ZONE 'UTC'), (now() AT TIME ZONE 'UTC'))
    """

    update_parent_query = """
    UPDATE bronze.themes
    SET num_id = $1, name = $2, updated_at = (now() AT TIME ZONE 'UTC')
    WHERE theme_id = $3
    """

    update_node_query = """
    UPDATE bronze.themes
    SET num_id = $1, name = $2, parent_id = $3, updated_at = (now() AT TIME ZONE 'UTC')
    WHERE theme_id = $4
    """

    conn.execute("BEGIN TRANSACTION")
    try:
        p_count = 0
        for p in parents:
            theme_id = p.get("key")
            if not theme_id:
                continue
            num_id = p.get("numId")
            name = p.get("name")
            if theme_id in existing_ids:
                conn.execute(update_parent_query, [num_id, name, theme_id])
            else:
                conn.execute(insert_query, [theme_id, num_id, name, None])
                existing_ids.add(theme_id)
            p_count += 1

        n_count = 0
        for n in nodes:
            theme_id = n.get("key")
            if not theme_id:
                continue
            num_id = n.get("numId")
            name = n.get("name")
            parent_id = n.get("parentKey")
            if theme_id in existing_ids:
                conn.execute(update_node_query, [num_id, name, parent_id, theme_id])
            else:
                conn.execute(insert_query, [theme_id, num_id, name, parent_id])
                existing_ids.add(theme_id)
            n_count += 1

        conn.execute("UPDATE bronze.themes SET last_checked_at = (now() AT TIME ZONE 'UTC')")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return p_count, n_count


async def sync(client: httpx.AsyncClient, force: bool = False) -> tuple[int, int]:
    """Fetches global theme taxonomy and updates bronze.themes."""
    async with AsyncDbWorker(settings.db_path) as worker:
        if not force:
            last_checked = await worker.submit(_check_themes_freshness)
            if last_checked and is_fresh(last_checked, settings.freshness_window_hours):
                now = datetime.now(UTC)
                if last_checked.tzinfo is None:
                    last_checked = last_checked.replace(tzinfo=UTC)
                seconds = max(0.0, (now - last_checked).total_seconds())
                hours = round(seconds / 3600.0, 1)
                console.info(f"Themes sync skipped (checked {hours}h ago; use --force to refresh).")
                return await worker.submit(_count_themes)

        logger.info("Syncing theme taxonomy from /tws.proxy/knowledge-graph/meta/themes...")
        url = "/tws.proxy/knowledge-graph/meta/themes"
        _, payload = await session.fetch_with_retry(client, url)
        p_count, n_count = await worker.submit(upsert_themes, payload)

    logger.info("Theme taxonomy synced successfully: %d parents, %d child nodes.", p_count, n_count)
    return p_count, n_count
