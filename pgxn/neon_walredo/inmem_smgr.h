/*-------------------------------------------------------------------------
 *
 * inmem_smgr.h
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#ifndef INMEM_SMGR_H
#define INMEM_SMGR_H

#if PG_MAJORVERSION_NUM >= 18
extern void inmem_smgr_register(void);
extern void inmem_smgr_reset(void);
#else
extern const f_smgr *smgr_inmem(ProcNumber backend, NRelFileInfo rinfo);
extern void smgr_init_inmem(void);
#endif

#endif /* INMEM_SMGR_H */
