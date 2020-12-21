/*-------------------------------------------------------------------------
 *
 * activity.h
 *	  Definitions for pg_stat_activity functions
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MC_ACTIVITY_H
#define MC_ACTIVITY_H

#include "common.h"

#define STAT_ACTIVITY_QUERY_SIZE            (1024)

/*
 * This enum defines the enum of BackendState
 * It should be exactly mapped with GPCC receiver side
 * Never change this enum alone
 */
typedef enum GPCCBackendState
{
	GPCC_STATE_UNDEFINED_OLD, // No more used, keep for backward compatibility
	GPCC_STATE_IDLE,
	GPCC_STATE_RUNNING,
	GPCC_STATE_IDLEINTRANSACTION,
	GPCC_STATE_FASTPATH,
	GPCC_STATE_IDLEINTRANSACTION_ABORTED,
	GPCC_STATE_DISABLED,
	GPCC_STATE_UNKNOWN
} GPCCBackendState;

typedef struct metrics_activity_t
{
	int32     magic;
	int32     version;
	int32     pkttype;
	int32     gp_segment_id;
	uint64    signature;
	int64     total;
	int64     length;
	TIMESTAMP timestamp;
	int32     changecnt;
	int32     procpid;
	int32     sess_id;
	int32     ccnt;
	double    act_start;
	double    query_start;
	double    backend_start;
	double    res_start;
	uint32    datid;
	uint32    usesysid;
	uint32    rsgid;
	uint32    tmid;
	char      application_name[NAMEDATALEN];
	char      current_query[STAT_ACTIVITY_QUERY_SIZE];
	int32     state;
	char      waiting;
	char      is_query_text_complete;
	int16     dmy;
} metrics_activity_t;

#endif /* MC_ACTIVITY_H */