import logging
from typing import Any

import duckdb
import httpx

from etfportfolio.core.db import current

logger = logging.getLogger(__name__)


def upsert_themes(conn: duckdb.DuckDBPyConnection, payload: dict[str, Any]) -> tuple[int, int]:
    """Upserts parents first, then nodes into bronze.themes.

    Returns (parents_count, nodes_count).
    """
    parents = payload.get("parents", [])
    nodes = payload.get("nodes", [])

    upsert_query = """
    INSERT INTO bronze.themes (theme_id, num_id, name, parent_id, created_at, updated_at)
    VALUES ($1, $2, $3, $4, now(), now())
    ON CONFLICT (theme_id) DO UPDATE SET
        num_id = EXCLUDED.num_id,
        name = EXCLUDED.name,
        parent_id = EXCLUDED.parent_id,
        updated_at = now()
    """

    # 1. Upsert parents first (parent_id is NULL)
    p_count = 0
    for p in parents:
        theme_id = p.get("key")
        if not theme_id:
            continue
        num_id = p.get("numId")
        name = p.get("name")
        conn.execute(upsert_query, [theme_id, num_id, name, None])
        p_count += 1

    # 2. Upsert child nodes second (parent_id is parentKey)
    n_count = 0
    for n in nodes:
        theme_id = n.get("key")
        if not theme_id:
            continue
        num_id = n.get("numId")
        name = n.get("name")
        parent_id = n.get("parentKey")
        conn.execute(upsert_query, [theme_id, num_id, name, parent_id])
        n_count += 1

    return p_count, n_count


async def sync(
    client: httpx.AsyncClient,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> tuple[int, int]:
    """Fetches global theme taxonomy and updates bronze.themes."""
    logger.info("Syncing theme taxonomy from /tws.proxy/knowledge-graph/meta/themes...")
    url = "/tws.proxy/knowledge-graph/meta/themes"
    resp = await client.get(url)

    if not resp.is_success:
        raise RuntimeError(f"Themes taxonomy sync failed with status {resp.status_code}: {resp.text}")

    payload = resp.json()
    db_conn = conn if conn is not None else current()
    p_count, n_count = upsert_themes(db_conn, payload)
    logger.info("Theme taxonomy synced successfully: %d parents, %d child nodes.", p_count, n_count)
    return p_count, n_count
