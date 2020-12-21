/*-------------------------------------------------------------------------
 *
 * metrics_collector.h
 *	  Definitions for metrics collector process.
 *
 * This file contains the basic interface that is needed by postmaster
 * to start the metrics collector process.
 *
 * Copyright (c) 2017-Present Pivotal Software, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MC_METRICS_COLLECTOR_H
#define MC_METRICS_COLLECTOR_H

#include "activity.h"
#include "common.h"
#include "lock.h"
#include "socket_utils.h"

/* Reset frame counter every 600ms */
#define METRICS_LOOP_TOTAL_FRAMES           (600)

/* Emit instr packet on frame = 10 every 50ms */
#define METRICS_LOOP_INSTRUMENT_INTERVAL    (50)
#define METRICS_LOOP_INSTRUMENT_FRAME       (10)

/* Emit spill file size packet on frame = 38 every 200ms */
#define METRICS_LOOP_SPILL_INTERVAL         (200)
#define METRICS_LOOP_SPILL_FRAME            (38)

/* Reset metrics collector memery context every 50ms */
#define METRICS_LOOP_CLEANUP_INTERVAL       (50)
#define METRICS_LOOP_CLEANUP_FRAME          (0)

#define SPILL_FILE_NUMBER_PER_UDP           (32)
#define INSTRUMENT_NUMBER_PER_UDP           (20)

typedef struct metrics_instrument_t
{
	int32  tmid;
	int32  ssid;
	int32  ccnt;
	int16  segid;
	int16  dmy;
	int32  nid;
	int32  pid;
	uint64 tuplecount;
	uint64 ntuples;
	uint64 nloops;
	bool   running;
	char   dmy2[7];
} metrics_instrument_t;

typedef struct metrics_spill_file_t
{
	int32  tmid;
	int32  sess_id;
	int32  ccnt;
	int32  slice_id;
	uint32 num_files;
	uint32 dmy;
	int64  workfile_size;
} metrics_spill_file_t;

typedef struct metrics_spill_pack
{
	int32                magic;
	int32                version;
	int32                pkttype;
	int32                gp_segment_id;
	uint64               signature;
	int64                total;
	int64                length;
	TIMESTAMP            timestamp;
	metrics_spill_file_t spill_file[SPILL_FILE_NUMBER_PER_UDP];
} metrics_spill_pack;

typedef struct metrics_instrument_pack
{
	int32                magic;
	int32                version;
	int32                pkttype;
	int32                gp_segment_id;
	uint64               signature;
	int64                total;
	int64                length;
	TIMESTAMP            timestamp;
	metrics_instrument_t instr[INSTRUMENT_NUMBER_PER_UDP];
} metrics_instrument_pack;

extern bool gpcc_enable_send_query_info;
extern bool gpcc_enable_query_profiling;
extern int32 gpcc_query_metrics_port;
extern int32 metrics_collector_pkt_version;
extern uint64 signature;
extern int current_bucket;
extern MemoryContext metricsCollectorContext;
extern metrics_instrument_pack nodeinfopkt;
extern metrics_lock_pack       lockinfopkt;
extern metrics_activity_t      activitypkt;
extern metrics_spill_pack      spillfilepkt;

#define EPOCH_OFFSET_SECONDS ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY)
#define TIMESTAMPTZ_TO_DOUBLE(t) \
    ((t) ? (double) (((double)t) / USECS_PER_SEC + EPOCH_OFFSET_SECONDS) : 0.0)

/* Interface */
extern void
MetricsCollectorLoopFunc(void);
extern void
InitNodeInstrPkt(metrics_instrument_pack *pkt);
extern void
InitSpillFilePkt(metrics_spill_pack *pkt);
extern void
InitLockInfoPkt(metrics_lock_pack *pkt);
extern void
UpdateLockInfoPkt(metrics_lock_pack *pkt);
extern void
InitStatActivityPkt(metrics_activity_t *pkt);
extern void
UpdateStatActivityPkt(metrics_activity_t *pkt, int maxLoop);
extern void
init_packet_buff_shmem(void);

#endif   /* MC_METRICS_COLLECTOR_H */
