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

    existing_ids = {row[0] for row in conn.execute("SELECT theme_id FROM bronze.themes").fetchall()}

    insert_query = """
    INSERT INTO bronze.themes (theme_id, num_id, name, parent_id, created_at, updated_at)
    VALUES (?, ?, ?, ?, now(), now())
    """

    update_parent_query = """
    UPDATE bronze.themes
    SET num_id = ?, name = ?, updated_at = now()
    WHERE theme_id = ?
    """

    update_node_query = """
    UPDATE bronze.themes
    SET num_id = ?, name = ?, parent_id = ?, updated_at = now()
    WHERE theme_id = ?
    """

    # 1. Upsert parents first (parent_id is NULL)
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

    # 2. Upsert child nodes second (parent_id is parentKey)
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
