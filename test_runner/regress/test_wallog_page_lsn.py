from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from fixtures.log_helper import log
from fixtures.pg_version import PgVersion
from fixtures.utils import USE_LFC, query_scalar, run_only_on_postgres

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv


@run_only_on_postgres(
    [PgVersion.V17],
    "neon_wallog_pagev() is PG17-only; older versions don't stamp the LSN on the evicted page",
)
@pytest.mark.skipif(
    not USE_LFC, reason="the LSN stamped on an evicted page is only observable in the LFC"
)
def test_wallog_fsm_page_lsn_on_eviction(neon_simple_env: NeonEnv):
    """
    FSM changes are not WAL-logged when they are made, so Neon's SMGR WAL-logs
    dirty FSM pages itself (as an XLOG_FPI record) when they are evicted from
    shared buffers. The LSN stamped on the page, which is also remembered as
    the page's last-written LSN, must be the *end* of that FPI record.
    Otherwise a later GetPage@LSN request can be served a page version from
    before the FPI, e.g. an FSM root page that reports no free space.

    The evicted page is read back from the LFC, so we see the LSN that Neon
    stamped on it, and then from the pageserver at that LSN.
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "neon.max_file_cache_size='64MB'",
            "neon.file_cache_size_limit='64MB'",
        ],
    )
    con = endpoint.connect()
    cur = con.cursor()
    cur.execute("CREATE EXTENSION neon_test_utils")
    cur.execute("CREATE EXTENSION pageinspect")
    cur.execute("CREATE EXTENSION pg_walinspect")

    cur.execute("CREATE TABLE t (id int, filler text) WITH (autovacuum_enabled = off)")
    cur.execute("INSERT INTO t SELECT g, repeat('x', 100) FROM generate_series(1, 2000) g")
    cur.execute("DELETE FROM t WHERE id % 2 = 0")

    start_lsn = query_scalar(cur, "SELECT pg_current_wal_insert_lsn()")

    # VACUUM records the freed space in the FSM, leaving dirty, un-WAL-logged FSM pages.
    cur.execute("VACUUM t")
    cur.execute(
        "SELECT pg_relation_filenode('t'), "
        "pg_relation_size('t', 'fsm') / current_setting('block_size')::int"
    )
    row = cur.fetchone()
    assert row is not None
    relfilenode, fsm_nblocks = row
    assert fsm_nblocks > 0

    # Evict everything; Neon's SMGR WAL-logs the dirty FSM pages on the way out.
    cur.execute("SELECT clear_buffer_cache()")
    # pg_walinspect only reads up to the flush LSN, and the FPIs written on eviction
    # are not necessarily flushed yet.
    cur.execute("SELECT neon_xlogflush()")
    end_lsn = query_scalar(cur, "SELECT pg_current_wal_flush_lsn()")

    checked = 0
    for blkno in range(fsm_nblocks):
        # Latest full-page image of this FSM block written since before the VACUUM.
        cur.execute(
            """
            SELECT start_lsn, end_lsn, record_type
            FROM pg_get_wal_block_info(%s::pg_lsn, %s::pg_lsn, false)
            WHERE relfilenode = %s AND relforknumber = 1 AND relblocknumber = %s
              AND record_type IN ('FPI', 'FPI_FOR_HINT')
            ORDER BY start_lsn DESC
            LIMIT 1
            """,
            (start_lsn, end_lsn, relfilenode, blkno),
        )
        row = cur.fetchone()
        if row is None:
            continue
        fpi_start, fpi_end, record_type = row

        # Read the page back in (from the LFC or the pageserver) and check its LSN.
        cur.execute("SELECT lsn FROM page_header(get_raw_page('t', 'fsm', %s))", (blkno,))
        row = cur.fetchone()
        assert row is not None
        page_lsn = row[0]
        log.info(
            f"FSM block {blkno}: {record_type} record {fpi_start}..{fpi_end}, page LSN {page_lsn}"
        )
        assert page_lsn == fpi_end, (
            f"FSM block {blkno}: page LSN {page_lsn} is not the end of its {record_type} "
            f"record ({fpi_start}..{fpi_end})"
        )

        # The pageserver must return the WAL-logged image when asked for the page
        # at the LSN Neon stamped on it (which is also its last-written LSN).
        cur.execute(
            "SELECT lsn FROM page_header(get_raw_page_at_lsn('t', 'fsm', %s, %s::pg_lsn, %s::pg_lsn))",
            (blkno, page_lsn, page_lsn),
        )
        row = cur.fetchone()
        assert row is not None
        ps_page_lsn = row[0]
        assert ps_page_lsn == fpi_end, (
            f"FSM block {blkno}: pageserver returned page version {ps_page_lsn} at LSN "
            f"{page_lsn}, older than its {record_type} record ({fpi_start}..{fpi_end})"
        )

        if record_type == "FPI":
            checked += 1

    assert checked > 0, "no FSM page was WAL-logged with an FPI record on eviction"
