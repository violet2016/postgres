/*-------------------------------------------------------------------------
 *
 * lock.h
 *	  Definitions for lock functions
 *
 * Copyright (c) 2018-Present Pivotal Software, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MC_LOCK_H
#define MC_LOCK_H

#include "common.h"

#define LOCKS_NUMBER_UDP                    (16)

/*
 * This enum defines the number of lock tag types
 * It should be exactly mapped with GPCC receiver side
 * Never change here alone
 * Refer: protos/query.proto EnumLockType
 */
typedef enum GPCCLockTagType
{
	GPCC_LOCKTAG_RELATION,
	GPCC_LOCKTAG_RELATION_EXTEND,
	GPCC_LOCKTAG_PAGE,
	GPCC_LOCKTAG_TUPLE,
	GPCC_LOCKTAG_TRANSACTION,
	GPCC_LOCKTAG_VIRTUALTRANSACTION,
	GPCC_LOCKTAG_RELATION_RESYNCHRONIZE,
	GPCC_LOCKTAG_RELATION_APPENDONLY_SEGMENT_FILE,
	GPCC_LOCKTAG_OBJECT,
	GPCC_LOCKTAG_RESOURCE_QUEUE,
	GPCC_LOCKTAG_USERLOCK,
	GPCC_LOCKTAG_ADVISORY
} GPCCLockTagType;

/*
 * Translate GPDB lock type to GPCC lock type. This is a safe
 * guard to not break GPCC when GPDB changed LockTagType enum
 */
#define SET_GPCC_LOCKTAG(lr, locktype) do {										\
	switch(locktype)															\
	{																			\
		case LOCKTAG_RELATION:													\
			lr->cclocktype = GPCC_LOCKTAG_RELATION;								\
			break;																\
		case LOCKTAG_RELATION_EXTEND:											\
			lr->cclocktype = GPCC_LOCKTAG_RELATION_EXTEND;						\
			break;																\
		case LOCKTAG_PAGE:														\
			lr->cclocktype = GPCC_LOCKTAG_PAGE;									\
			break;																\
		case LOCKTAG_TUPLE:														\
			lr->cclocktype = GPCC_LOCKTAG_TUPLE;								\
			break;																\
		case LOCKTAG_TRANSACTION:												\
			lr->cclocktype = GPCC_LOCKTAG_TRANSACTION;							\
			break;																\
		case LOCKTAG_VIRTUALTRANSACTION:										\
			lr->cclocktype = GPCC_LOCKTAG_VIRTUALTRANSACTION;					\
			break;																\
		case LOCKTAG_OBJECT:													\
			lr->cclocktype = GPCC_LOCKTAG_OBJECT;								\
			break;																\
		case LOCKTAG_USERLOCK:													\
			lr->cclocktype = GPCC_LOCKTAG_USERLOCK;								\
			break;																\
		case LOCKTAG_ADVISORY:													\
			lr->cclocktype = GPCC_LOCKTAG_ADVISORY;								\
			break;																\
		default:																\
			set_more_locktypes(lr, locktype);									\
			break;																\
	}																			\
} while(0)

typedef struct metrics_lock_t
{
	/* Virtual ID of the transaction targeted by the lock, backend ID part */
	int32   xbackendid;
	/*
	 * Virtual ID of the transaction that is holding or awaiting this lock,
	 * backend ID part
	 */
	int32   tbackendid;
	/*
	 * Process ID of the server process holding or awaiting this lock,
	 * or null if the lock is held by a prepared transaction
	 */
	int32   pid;

	/*
	 * Lock mode held or desired by this process 0=INVALID, 1=AccessShareLock,
	 * 2=RowShareLock, 3=RowExclusiveLock, 4=ShareUpdateExclusiveLock,
	 * 5=ShareLock, 6=ShareRowExclusiveLock, 7=ExclusiveLock,
	 * 8=AccessExclusiveLock
	 */
	int32   mode;
	int32   tmid;

	/* serial num of the qDisp process */
	int32   mppSessionId;
	/* command_id for the running query */
	int32   queryCommandId;
	/*
	 * OID of the database in which the lock target exists, or zero if the
	 * target is a shared object, or null if the target is a transaction ID
	 */
	uint32  database;
	/*
	 * OID of the relation targeted by the lock, or null if the target is
	 * not a relation or part of a relation
	 */
	uint32  relation;

	/*
	 * Virtual ID of the transaction targeted by the lock,
	 * local transaction ID part
	 */
	uint32  xlocaltransactionid;
	/*
	 * ID of the transaction targeted by the lock, or null if the target is
	 * not a transaction ID
	 */
	uint32  transactionid;
	/*
	 * OID of the system catalog containing the lock target, or null if
	 * the target is not a general database object
	 */
	uint32  classid;
	/*
	 * OID of the lock target within its system catalog, or null if
	 * the target is not a general database object
	 */
	uint32  objid;
	/*
	 * Virtual ID of the transaction that is holding or awaiting this lock,
	 * local transaction ID part
	 */
	uint32  tlocaltransactionid;

	/*
	 * Page number targeted by the lock within the relation, or null if
	 * the target is not a relation page or tuple
	 */
	uint16  page;
	/*
	 * Type of the lockable object: 0=relation, 1=extend, 2=page, 3=tuple,
	 * 4=transactionid, 5=virtualxid, 6=resynchronize, 7=append-only-seg-file,
	 * 8=object, 9=resource queue, 10=userlock, 11=advisory, see GPCCLockTagType
	 */
	uint8   cclocktype;

	/*
	 * Tuple number targeted by the lock within the page, or null if
	 * the target is not a tuple
	 */
	uint8   tuple;
	/*
	 * Column number targeted by the lock (the classid and objid refer to
	 * the table itself), or zero if the target is some other general database
	 * object, or null if the target is not a general database object
	 */
	uint8   objsubid;
	/* 0=INVALID, 1=default, 2=user, 3=resource */
	uint8   methodid;
	/* True if lock is held, false if lock is awaited */
	bool    granted;
	/* The writer gang member, holder of locks */
	bool    mppIsWriter;
} metrics_lock_t;

typedef struct metrics_lock_pack
{
	int32          magic;
	int32          version;
	int32          pkttype;
	int32          gp_segment_id;
	uint64         signature;
	int64          total;
	int64          length;
	TIMESTAMP      timestamp;
	metrics_lock_t lock[LOCKS_NUMBER_UDP];
} metrics_lock_pack;

/* Interface */
extern List*
LockFilter(List* llist);
extern void
populate_lock_tag(metrics_lock_t *lr, LOCKTAG *tag);

/* Interface by utils_gp.c */
extern metrics_lock_t*
populate_lock_data(LockData *lockData, int *i, int tmid);

#endif /* MC_LOCK_H */