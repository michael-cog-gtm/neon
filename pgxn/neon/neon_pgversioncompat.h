/*
 * Compatibility macros to cover up differences between supported PostgreSQL versions,
 * to help with compiling the same sources for all of them.
 */

#ifndef NEON_PGVERSIONCOMPAT_H
#define NEON_PGVERSIONCOMPAT_H

#include "fmgr.h"
#include "storage/buf_internals.h"

#if PG_MAJORVERSION_NUM < 16
typedef PGAlignedBlock PGIOAlignedBlock;
#endif

#if PG_MAJORVERSION_NUM < 17
#define NRelFileInfoBackendIsTemp(rinfo) (rinfo.backend != InvalidBackendId)
#else
#define NRelFileInfoBackendIsTemp(rinfo) (rinfo.backend != INVALID_PROC_NUMBER)
#endif

#define RelFileInfoEquals(a, b) ( \
	NInfoGetSpcOid(a) == NInfoGetSpcOid(b) && \
	NInfoGetDbOid(a) == NInfoGetDbOid(b) && \
	NInfoGetRelNumber(a) == NInfoGetRelNumber(b) \
)

/* These macros were turned into static inline functions in v16 */
#if PG_MAJORVERSION_NUM < 16
static inline bool
BufferTagsEqual(const BufferTag *tag1, const BufferTag *tag2)
{
	return BUFFERTAGS_EQUAL(*tag1, *tag2);
}

static inline void
InitBufferTag(BufferTag *tag, const RelFileNode *rnode,
			  ForkNumber forkNum, BlockNumber blockNum)
{
	INIT_BUFFERTAG(*tag, *rnode, forkNum, blockNum);
}
#endif

/* RelFileNode -> RelFileLocator rework */
#if PG_MAJORVERSION_NUM < 16
#define USE_RELFILENODE

#define RELFILEINFO_HDR "storage/relfilenode.h"

#define NRelFileInfo RelFileNode
#define NRelFileInfoBackend RelFileNodeBackend
#define NRelFileNumber Oid

#define InfoFromRelation(rel) (rel)->rd_node
#define InfoFromSMgrRel(srel) (srel)->smgr_rnode.node
#define InfoBFromSMgrRel(srel) (srel)->smgr_rnode
#define InfoFromNInfoB(ninfob) ninfob.node

#define RelFileInfoFmt(rinfo) \
	(rinfo).spcNode, \
	(rinfo).dbNode, \
	(rinfo).relNode

#define RelFileInfoBackendFmt(ninfob) \
	(ninfob).backend, \
	(ninfob).node.spcNode, \
	(ninfob).node.dbNode, \
	(ninfob).node.relNode

#define NInfoGetSpcOid(ninfo)		(ninfo).spcNode
#define NInfoGetDbOid(ninfo)		(ninfo).dbNode
#define NInfoGetRelNumber(ninfo)	(ninfo).relNode

#define CopyNRelFileInfoToBufTag(tag, rinfo) \
	do { \
		(tag).rnode = (rinfo); \
	} while (false)

#define BufTagGetNRelFileInfo(tag) (tag).rnode

#define BufTagGetRelNumber(tagp) ((tagp)->rnode.relNode)

#define BufTagInit(tag, rel_number, fork_number, block_number, spc_oid, db_oid) \
	do { \
		RelFileNode rnode = { .spcNode = (spc_oid), .dbNode = (db_oid), .relNode = (rel_number)}; \
		(tag).forkNum = (fork_number);								\
		(tag).blockNum = (block_number);							\
		(tag).rnode = rnode;										\
	} while (false)

#define InvalidRelFileNumber InvalidOid

#define SMgrRelGetRelInfo(reln)	   	((reln)->smgr_rnode.node)

#define DropRelationAllLocalBuffers DropRelFileNodeAllLocalBuffers

#else							/* major version >= 16 */

#define USE_RELFILELOCATOR

#define RELFILEINFO_HDR "storage/relfilelocator.h"

#define NRelFileInfo RelFileLocator
#define NRelFileInfoBackend RelFileLocatorBackend

#define InfoFromRelation(rel) (rel)->rd_locator
#define InfoFromSMgrRel(srel) (srel)->smgr_rlocator.locator
#define InfoBFromSMgrRel(srel) (srel)->smgr_rlocator
#define InfoFromNInfoB(ninfob) (ninfob).locator

#define RelFileInfoFmt(rinfo) \
	(rinfo).spcOid, \
	(rinfo).dbOid, \
	(rinfo).relNumber
#define RelFileInfoBackendFmt(ninfob) \
	(ninfob).backend, \
	(ninfob).locator.spcOid, \
	(ninfob).locator.dbOid, \
	(ninfob).locator.relNumber

#define NInfoGetSpcOid(ninfo)		(ninfo).spcOid
#define NInfoGetDbOid(ninfo)		(ninfo).dbOid
#define NInfoGetRelNumber(ninfo)	(ninfo).relNumber

#define CopyNRelFileInfoToBufTag(tag, rinfo) \
	do { \
		(tag).spcOid = (rinfo).spcOid; \
		(tag).dbOid = (rinfo).dbOid; \
		(tag).relNumber = (rinfo).relNumber; \
	} while (false)

#define BufTagGetNRelFileInfo(tag) \
	((RelFileLocator) { \
		.spcOid = (tag).spcOid, \
		.dbOid = (tag).dbOid, \
		.relNumber = (tag).relNumber, \
	})

#define BufTagInit(tag, rel_number, fork_number, block_number, spc_oid, db_oid) \
	do { \
		(tag).forkNum = (fork_number);					\
		(tag).blockNum = (block_number);				\
		(tag).spcOid = (spc_oid);						\
		(tag).dbOid = (db_oid);							\
		(tag).relNumber = (rel_number);					\
	} while (false)

#define SMgrRelGetRelInfo(reln)	   	((reln)->smgr_rlocator)

#define DropRelationAllLocalBuffers DropRelationAllLocalBuffers
#endif

#define NRelFileInfoInvalidate(rinfo) do { \
		NInfoGetSpcOid(rinfo) = InvalidOid; \
		NInfoGetDbOid(rinfo) = InvalidOid; \
		NInfoGetRelNumber(rinfo) = InvalidRelFileNumber; \
	} while (0)

#if PG_MAJORVERSION_NUM < 17
#define ProcNumber BackendId
#define INVALID_PROC_NUMBER InvalidBackendId
#define AmAutoVacuumWorkerProcess() (IsAutoVacuumWorkerProcess())
#endif

#if PG_MAJORVERSION_NUM < 17
#define	MyProcNumber (MyProc - &ProcGlobal->allProcs[0])
#endif

#if PG_MAJORVERSION_NUM < 15
extern void InitMaterializedSRF(FunctionCallInfo fcinfo, bits32 flags);
extern TimeLineID GetWALInsertionTimeLine(void);
#endif

/* format codes not present in PG17-; but available in PG18+ */
#if PG_MAJORVERSION_NUM < 18
#define INT64_HEX_FORMAT "%" INT64_MODIFIER "x"
#define UINT64_HEX_FORMAT "%" INT64_MODIFIER "x"
#else
/* PG18 dropped INT64_MODIFIER in favour of <inttypes.h> macros */
#ifndef INT64_HEX_FORMAT
#define INT64_HEX_FORMAT "%" PRIx64
#endif
#ifndef UINT64_HEX_FORMAT
#define UINT64_HEX_FORMAT "%" PRIx64
#endif
#endif

/* PG18 relpath() returns a RelPathStr struct instead of a palloc'd string */
#if PG_MAJORVERSION_NUM >= 18
#define neon_relpath(rlocator, forknum) (relpath(rlocator, forknum).str)
#else
#define neon_relpath(rlocator, forknum) relpath(rlocator, forknum)
#endif

#if PG_MAJORVERSION_NUM >= 18
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/aio.h"
#include "storage/aio_internal.h"
#include "storage/smgr.h"

/*
 * PG18 reads shared buffers through the AIO subsystem: bufmgr hands the smgr
 * an AIO handle with completion callbacks already registered, and the smgr's
 * smgr_startreadv() is expected to stage an IO on it.  Neon (and the walredo
 * in-memory smgr) don't read from a local file descriptor, so instead we
 * fill the buffers synchronously first (with neon_readv / inmem_readv) and
 * then drive the handle through the same state transitions as
 * pgaio_io_stage() + pgaio_io_perform_synchronously() would, reporting all
 * 'nblocks' as read. No smgr-level completion callback is registered, so the
 * result is already expressed in blocks, as the buffer-level callbacks
 * expect.
 */
static inline void
neon_aio_set_state(PgAioHandle *ioh, PgAioHandleState new_state)
{
	Assert(!INTERRUPTS_CAN_BE_PROCESSED());
	pg_write_barrier();
	ioh->state = new_state;
}

static inline void
neon_aio_complete_readv_synchronously(PgAioHandle *ioh, SMgrRelation reln,
									  ForkNumber forknum, BlockNumber blocknum,
									  void **buffers, BlockNumber nblocks)
{
	struct iovec *iov;
	int			iovcnt PG_USED_FOR_ASSERTS_ONLY;

	Assert(ioh->state == PGAIO_HS_HANDED_OUT);

	iovcnt = pgaio_io_get_iovec(ioh, &iov);
	Assert(nblocks <= iovcnt);
	for (BlockNumber i = 0; i < nblocks; i++)
	{
		iov[i].iov_base = buffers[i];
		iov[i].iov_len = BLCKSZ;
	}

	pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks, false);
	ioh->op_data.read.fd = -1;
	ioh->op_data.read.iov_length = nblocks;
	ioh->op_data.read.offset = (uint64) blocknum * BLCKSZ;

	HOLD_INTERRUPTS();

	ioh->op = PGAIO_OP_READV;
	ioh->result = 0;
	neon_aio_set_state(ioh, PGAIO_HS_DEFINED);
	pgaio_my_backend->handed_out_io = NULL;
	pgaio_io_call_stage(ioh);
	neon_aio_set_state(ioh, PGAIO_HS_STAGED);
	pgaio_io_prepare_submit(ioh);

	START_CRIT_SECTION();
	pgaio_io_process_completion(ioh, (int) nblocks);
	END_CRIT_SECTION();

	RESUME_INTERRUPTS();
}
#endif

#endif							/* NEON_PGVERSIONCOMPAT_H */
