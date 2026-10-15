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
- (in progress)

## Test status
- (in progress)
