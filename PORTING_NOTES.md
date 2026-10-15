# PostgreSQL 18 port – notes

Sources: this repo, `neondatabase/postgres` branch `REL_18_STABLE_neon`
(pinned at `a616eefea763784218a67f81395ba95bc2cf7c4d`, PG 18.2), PG18 release notes and PG source.

## PG18 changes that needed handling

### Build / vendoring
- `vendor/postgres-v18` submodule (`REL_18_STABLE_neon`), `vendor/revisions.json`, `POSTGRES_VERSIONS` in `Makefile`.

### SMGR API (Neon fork, PG18)
The PG18 fork replaced the `smgr_hook`/`smgr_init_hook` + `smgr_standard()` model with a registry:
- `smgrregister(const f_smgr *)` returns an `SmgrId`; `smgropen()` asks each registered smgr (latest first) via the new
  `smgr_owns(rlocator, backend, relpersistence)` callback. `f_smgr` gained a required `smgr_name`.
  - `pgxn/neon`: `neon_smgr_register()` registers `neon_smgr`; `neon_owns()` claims all non-temp relations (same split as
    PG17's `smgr_neon()`); unlogged / unlogged-build relations are still delegated to md.c internally.
  - `pgxn/neon_walredo`: `inmem_smgr_register()`; `inmem_owns()` claims everything.
  - md.c init is now done by core (`smgr_init_standard()` is gone).
- Unlogged index builds are no longer smgr methods: `start_unlogged_build_hook`, `finish_unlogged_build_phase_1_hook`,
  `end_unlogged_build_hook` globals. They're called for every relation, so the neon wrappers skip temp relations.
- SLRU on-demand download: `read_slru_segment_hook(path, int64 segno) -> bool`; the hook now has to write the segment file
  itself (PG17 core did that from the returned buffer).
- New `f_smgr` members: `smgr_maxcombine`, `smgr_startreadv`, `smgr_fd`.

### Asynchronous I/O (PG18 core)
PG18 reads shared buffers through AIO (`AsyncReadBuffers()` -> `smgrstartreadv()` with a `PgAioHandle` that already has
buffer-completion callbacks registered). Neon has no local fd for pageserver-backed pages, so:
- `neon_startreadv()` reads synchronously via `neon_readv()` (LFC/prefetch/pageserver as before), then
  `neon_aio_complete_readv_synchronously()` (in `neon_pgversioncompat.h`) drives the handle through the same transitions as
  `pgaio_io_stage()` + `pgaio_io_perform_synchronously()` (DEFINED -> stage callbacks -> STAGED -> SUBMITTED ->
  `pgaio_io_process_completion()`), reporting `nblocks` (no md callback is registered, so the result is in blocks).
  `pgaio_io_update_state()` is static, so the state store + write barrier is replicated.
- Local-file relations (temp/unlogged/unlogged-build phase 1) go to `mdstartreadv()` (real AIO); `neon_fd()` = `mdfd()`
  so IO workers can reopen those files.
- `smgr_maxcombine`: md's limit for local relations, `PG_IOV_MAX` otherwise.
- walredo: `inmem_startreadv()` uses the same helper; walredo forces `io_method=sync`, calls `AioShmemInit()` and
  `pgaio_init_backend()`.

### Other core API changes hit by pgxn
- `WL_*` flags moved to `storage/waiteventset.h`; files that relied on transitive includes now include `storage/latch.h`.
- `INT64_MODIFIER` removed -> `INT64_HEX_FORMAT`/`UINT64_HEX_FORMAT` from `PRIx64`.
- `relpath()` returns `RelPathStr` -> `neon_relpath()` compat macro.
- `ReplicationSlotAcquire()` gained `error_if_invalid`.
- Shmem init functions renamed (`InitBufferPool` -> `BufferManagerShmemInit`, `InitLocks` -> `LockManagerShmemInit`,
  `InitPredicateLocks` -> `PredicateLockShmemInit`, `CreateSharedProcArray` -> `ProcArrayShmemInit`,
  `CreateSharedBackendStatus` -> `BackendStatusShmemInit`, `CreateSharedInvalidationState` -> `SharedInvalShmemInit`) in walredo's fake shmem setup.
- `smgrinit()` now registers an `on_proc_exit` callback each call; walredo's per-record reset uses `inmem_smgr_reset()`.
- Last-written-LSN cache: the PG18 fork dropped the block-level `set_lwlsn_block*` hooks and the `LastWrittenLsn` core
  LWLock; neon allocates its own `neon_lwlsn` tranche and only installs the db/relation/max hooks.

### WAL / on-disk formats
- `XLOG_PAGE_MAGIC` 0xD118, `PG_CONTROL_VERSION` 1800, `CATALOG_VERSION_NO` 202506291 (picked up from bindings;
  catversion added to import_pgdata's version detection).
- `ControlFileData` gained `default_char_signedness` right before `crc`. Neon used the v14 `ControlFileData` re-export
  version-agnostically when importing a fresh initdb datadir, so PG18 `tenant create` failed with
  `invalid CRC in control file`. Added `postgres_ffi::decode_pg_control()`, which verifies the CRC with the file's own
  layout (by `pg_control_version`) and returns the shared-prefix fields; used by `import_datadir` and `import_pgdata`.
- `xl_heap_inplace` now carries invalidation messages; neon only needs its block reference, so no decoder change.
- Heap/neon-rmgr/dbase/XLOG_PARAMETER_CHANGE/END_OF_RECOVERY record layouts are unchanged vs PG17; the v17 decoders are
  reused for PG18.
- `xl_xact_stats_item` grew from 12 to 16 bytes (`objid` split into `objid_lo`/`objid_hi`). `XlXactParsedRecord::decode`
  skipped dropped-stats items with a hard-coded 12, so any commit that dropped a relation was mis-parsed. The safekeeper's
  interpreted-WAL reader panicked (`cannot advance past remaining`), the pageserver's walreceiver kept disconnecting, and
  getpage requests timed out with `LSN timeout`. `decode` now takes the `PgMajorVersion` and uses 16 bytes on PG18.

### Multixact offsets: next multixid's offset is pre-set
- The vendored 18.2 `RecordNewMultiXact` also writes the *next* multixid's offset (end of this multixid's members). It
  extends the offsets SLRU for `multi + 1` up front, so readers no longer have to wait for the next multixid to be
  assigned. The pageserver's `MultixactOffsetCreate` redo only wrote the multixid's own entry. After pg_regress,
  `check_restored_datadir_content` found `pg_multixact/offsets/0000` differing in exactly that next entry. On PG18,
  `ingest_multixact_create` now also sets the offset for `mid + 1`, skipping 0 for both id and offset as PG does. The
  pageserver allows only one record per key and LSN (a second one is dropped with `written twice at same LSN`). So when
  both entries are on the same page, it emits a new `NeonWalRecord::MultixactOffsetCreateWithNext { mid, moff, next_moff }`.
  When they're on different pages, it emits one `MultixactOffsetCreate` per page. The new variant goes before the
  `cfg(testing)` `Test` variant so the persisted enum tags of non-test builds keep their values. The vendored PG14-17 sources do not have this change, so the record is gated on PG18. The PG18 code says
  older minors lacked it, so it is a back-patched fix. If it also reaches the PG14-17 branches, bumping those
  submodules will need the gate widened.

### initdb: data checksums on by default
- PG18's `initdb` enables data checksums by default. Pages that the pageserver reconstructs don't carry valid checksums,
  so computes failed with `invalid page in block N of relation ...` once they read a page back from the pageserver.
  `postgres_initdb` (pageserver/compute initdb) and `wal_craft` now pass `--no-data-checksums` on PG18, which matches
  the PG14-17 clusters.

### Unlogged index builds vs. buffer eviction by other backends
- GiST/SP-GiST/GIN builds keep their pages in a local file during phase 1. Those pages are not WAL-logged and have LSN 0
  (SP-GiST, GIN) or `GistBuildLSN` (1). Only the building backend knows a build is in progress. On PG18, pg_regress hit
  shared-buffer evictions of such pages by *other* backends. They PANICked with
  `Page N of relation ... is evicted with zero LSN` (SP-GiST, page id 0xFF82), or failed
  `Assert(lsn >= WalSegMinSize)` in `neon_set_lwlsn_block_v` (GiST). On PG18, `neon_writev` now checks permanent
  main-fork pages whose LSN is below `FirstNormalUnloggedLSN`: if the relation's local file exists, the page belongs to
  another backend's build and is written there.

### walredo: postmaster child slots
- PG18 sizes PMSignal shared memory from `MaxLivePostmasterChildren()`, which errors with
  `PM child array not initialized yet` unless `InitPostmasterChildSlots()` ran (the postmaster, bootstrap and
  single-user mode all call it). neon_walredo builds its own pseudo-shmem, so every WAL redo request died. That made
  computes reconnect in a tight loop: pgbench hung and the pageserver log grew to 12 GB. `walredoproc.c` now calls
  `InitPostmasterChildSlots()` right after `InitializeMaxBackends()` on PG18, as single-user mode does.
- PG18 split `InitLocks()` into `LockManagerShmemInit()` (shared) and `InitLockManagerAccess()` (backend-local
  `LockMethodLocalHash`, called from `BaseInit()`). walredo doesn't run `BaseInit()`, so the local hash stayed NULL.
  The first redo that extended the visibility map (`visibilitymap_pin` -> `vm_extend` -> `LockRelationForExtension`)
  segfaulted in `hash_search`, and the pageserver only saw `early eof` on walredo stdout. walredo now calls
  `InitLockManagerAccess()` after `smgrinit()`. Checked by replaying a saved failing `.walredo` input: exit 0,
  8192-byte page returned.

### Logical decoding (neon_rmgr)
- PG18 renamed `ReorderBufferGetChange`/`ReorderBufferGetTupleBuf` to `ReorderBufferAllocChange`/`ReorderBufferAllocTupleBuf`.
  `neon_rmgr_decode.c` only compiled its decoder for `== 16` / `== 17`, so on PG18 `neon_rmgr.so` failed to load
  (`undefined symbol: neon_rm_decode`). The PG17 decoder is now used for `>= 17`, with aliases for the renamed functions.

### Rust
- `PgMajorVersion::PG18`, `PgVersionId` range, `for_all_postgres_versions!`/dispatch macros, bindgen for v18.

## Skipped / left for later
- **Real asynchronous I/O.** `neon_startreadv` performs reads synchronously and completes the AIO handle inline.
  Prefetching still goes through the existing communicator path. `io_method=worker/io_uring` are not exercised against
  neon smgr.
- **`debug_compare_local` with foreign unlogged-build pages.** The PG18 fallback in `neon_writev` is disabled when
  `debug_compare_local` is on, because that mode keeps its own local copies.
- **Data checksums.** They are disabled on PG18, as on PG14-17. Supporting `data_checksums=on` would need the pageserver
  to keep checksums valid in reconstructed pages.
- **Extensions outside `pgxn/`.** The compute image's third-party extensions (`compute/` Dockerfile) were not built for
  PG18.
- **Test coverage.** Only the curated suite below was run. The full `test_runner` regression suite and the other
  `test_pg_regress` parameters (`v2` reldir, `shard_count=4`) have not been run on PG18.
- **CI.** Nothing has been verified in CI. All results below are local debug builds.

## Test status
All runs are local `BUILD_TYPE=debug` builds made with `make -j CARGO_BUILD_FLAGS="--features testing"`.

Curated suite (`~/notes/run_suite.sh`):
`test_neon_cli_basics`, `test_branching_with_pgbench`, `test_pageserver_restart`, `test_createdb`, `test_dropdb`,
`test_wal_acceptor.py::{test_restarts,test_restart_endpoint,test_restart_endpoint_wal_page_boundary}` and
`test_pg_regress[v1-None]`. The 3 deselected tests are the other `test_pg_regress` parameters (`v2` and/or `shard_count=4`).

| Suite | PG18 | PG17 |
|---|---|---|
| curated (11 tests) | `11 passed, 3 deselected in 1087.81s`, exit 0 | `11 passed, 3 deselected in 817.04s`, exit 0 |

Builds and Rust unit tests (run after the final changes):
- `make -j8 CARGO_BUILD_FLAGS="--features testing"`: exit 0, and `neon.so` was rebuilt for v14, v15, v16, v17 and v18.
- `cargo nextest run --features testing -p postgres_ffi -p wal_decoder -p pageserver -p safekeeper`:
  `340 tests run: 340 passed, 0 skipped`.
