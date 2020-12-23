/*-------------------------------------------------------------------------
 *
 * metrics_collector.c
 * Send query metrics information to agent
 *
 * This file contains functions for sending query metrics information
 * to agent. At startup the postmaster process forks a new process
 * that sends query metrics using UDP packets.
 *
 * Copyright (c) 2017-Present Pivotal Software, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include <unistd.h>
#include <sys/stat.h>

#include "metrics_collector.h"
#include "utils.h"
#include "executor/instrument.h"
#include "storage/ipc.h"
#include "postmaster/bgworker.h"

PG_MODULE_MAGIC;

extern void
_PG_init(void);
void
_PG_fini(void);

static void
UpdateNodeInstrPkt(metrics_instrument_pack *pkt);
static bool
ShouldUpdateLocksInfo(int max);
static void
UpdateSpillFilePkt(metrics_spill_pack *pkt);

static void
packet_buff_shmem_startup(void);
static void
BatchSendPackets(void);

static shmem_startup_hook_type prev_packet_shmem_startup_hook = NULL;

static int    frame                 = 0;
static uint64 loopcnt               = 0;
static bool   delayedActivity       = false;
static uint64 nextSendActivityFrame = 10;
static uint64 nextSendLockFrame     = 55;
static int    act_max;

metrics_instrument_pack nodeinfopkt;
metrics_lock_pack       lockinfopkt;
metrics_activity_t      activitypkt;
metrics_spill_pack      spillfilepkt;

uint64 signature;

MemoryContext metricsCollectorContext = NULL;
bool gpcc_enable_send_query_info = true;
bool gpcc_enable_query_profiling = false;
int32 gpcc_query_metrics_port = 9898;
int32 metrics_collector_pkt_version = METRICS_PACKET_VERSION;

PacketBuffer *pbuff;
int current_bucket = 0;

void
MetricsCollectorLoopFunc(void)
{
	MemoryContext oldContext = NULL;
// FIXME: tmid
/*
	if (tmid == 0)
	{
		tmid = getDtxStartTime();
		if (tmid == 0)
		{
			// GPDB is not ready, skip this call
			return;
		}
*/
		/*
		elog(LOG, "Metrics Collector: size of metrics_lock_t              (%lu)", sizeof(metrics_lock_t));
		elog(LOG, "Metrics Collector: size of metrics_lock_pack           (%lu)", sizeof(metrics_lock_pack));
		elog(LOG, "Metrics Collector: size of metrics_activity_t          (%lu)", sizeof(metrics_activity_t));
		elog(LOG, "Metrics Collector: size of metrics_spill_file_t        (%lu)", sizeof(metrics_spill_file_t));
		elog(LOG, "Metrics Collector: size of metrics_spill_pack          (%lu)", sizeof(metrics_spill_pack));
		elog(LOG, "Metrics Collector: size of metrics_instrument_t        (%lu)", sizeof(metrics_instrument_t));
		elog(LOG, "Metrics Collector: size of metrics_instrument_pack     (%lu)", sizeof(metrics_instrument_pack));
		elog(LOG, "Metrics Collector: size of metrics_query_info          (%lu) - %d", sizeof(metrics_query_info), NAMEDATALEN * 3);
		elog(LOG, "Metrics Collector: size of metrics_error_query_info    (%lu) - %d", sizeof(metrics_error_query_info), MAX_STRING_LEN);
		elog(LOG, "Metrics Collector: size of metrics_plan_init_node      (%lu) - %d", sizeof(metrics_plan_init_node), NAMEDATALEN * 4);
		elog(LOG, "Metrics Collector: size of metrics_plan_init_node_cond (%lu) - %d - %d", sizeof(metrics_plan_init_node_with_condition), NAMEDATALEN * 4, MAX_STRING_LEN);
		elog(LOG, "Metrics Collector: size of metrics_plan_start_node     (%lu)", sizeof(metrics_plan_start_node));
		elog(LOG, "Metrics Collector: size of metrics_plan_done_node      (%lu)", sizeof(metrics_plan_done_node));
		elog(LOG, "Metrics Collector: size of metrics_query_text          (%lu) - %d", sizeof(metrics_query_text), MAX_STRING_LEN);
		*/

		//signature = (uint64) tmid * 10;
	//}

	if (should_init_socket())
		pbuff->main_connected = (socket_init() >= 0);

	// do nothing if agent not start
	if (should_keep_silence())
		return;

	oldContext = MemoryContextSwitchTo(metricsCollectorContext);
	signature++;
	loopcnt++;
	frame++;
	frame = (frame >= METRICS_LOOP_TOTAL_FRAMES) ? 0 : frame;

	/* Send instrument info */
	if (METRICS_LOOP_INSTRUMENT_FRAME ==
		frame % METRICS_LOOP_INSTRUMENT_INTERVAL)
	{
		UpdateNodeInstrPkt(&nodeinfopkt);
	}

	/* Update activity info */
	if (loopcnt == nextSendActivityFrame)
	{
		if (delayedActivity == false)
		{
			/* Clear local data of activity snapshot */
			pgstat_clear_snapshot();
			/* Take a snapshot of current stat_activity data */
			act_max = pgstat_fetch_stat_numbackends();
		}

		if (ShouldUpdateLocksInfo(act_max) && delayedActivity == false)
		{
			// Here we found activity info change require intercept locks update
			// We send locks immediately and delay activities for 0.1 sec
			UpdateLockInfoPkt(&lockinfopkt);
			nextSendActivityFrame += 1;
			delayedActivity = true;

			// Book next update for lock
			nextSendLockFrame = loopcnt + 45;
		}
		else
		{
			delayedActivity = false;
			UpdateStatActivityPkt(&activitypkt, act_max);

			// Send Activity every 1 sec
			nextSendActivityFrame += 20;
		}
	}

	/* Send lock info */
	if (loopcnt == nextSendLockFrame)
	{
		// If no activity changes calls for intercept locks update
		// Should periodically update locks data here
		UpdateLockInfoPkt(&lockinfopkt);
		// Send Locks every 5 sec
		nextSendLockFrame = loopcnt + 50;
	}

	/* Send spill info */
	if (METRICS_LOOP_SPILL_FRAME == frame % METRICS_LOOP_SPILL_INTERVAL)
	{
		UpdateSpillFilePkt(&spillfilepkt);
	}

	if (!delayedActivity &&
		METRICS_LOOP_CLEANUP_FRAME == frame % METRICS_LOOP_CLEANUP_INTERVAL)
	{
		MemoryContextReset(metricsCollectorContext);
	}

	MemoryContextSwitchTo(oldContext);

	BatchSendPackets();
}

static void
BatchSendPackets(void)
{
	PacketBufferHeader	   *pb_header;
	int						bytes_to_send = 0;
	int						batches_to_send = 0;
	int						i;
	batch_packet_header	   *send[NUM_PACKET_BUCKET];
	batch_packet_header	   *buff;

	if (flushBuffContext == NULL)
		flushBuffContext = AllocSetContextCreate(TopMemoryContext,
		                                         "MetricsFlushBuffMemCtxt",
		                                         ALLOCSET_DEFAULT_INITSIZE,
		                                         ALLOCSET_DEFAULT_INITSIZE,
		                                         ALLOCSET_DEFAULT_MAXSIZE);

	MemoryContextReset(flushBuffContext);
	MemoryContext oldcontext = MemoryContextSwitchTo(flushBuffContext);
	buff = (batch_packet_header*) palloc0(sizeof(batch_packet_header) + packet_buffer_size_actual + FLUSH_BUFF_GUARD);

	/**
	 * Try to combine multi buckets into one packet which less than the send size limit.
	 */
	for (i = 0; i < NUM_PACKET_BUCKET; i++)
	{
		pb_header = &pbuff->h[i];

		SpinLockAcquire(&pb_header->lock);

		if (pb_header->current_pos > 0)
		{
			if (pb_header->current_pos > packet_buffer_size_red_line_actual)
			{
				/**
				 * This should never happen, but in MPP-30447 MPP-30545 we doubt
				 * there maybe memory corruption in flushBuffContext.
				 * So we check and log if any exception happens.
				 */
				elog(LOG, "Metrics collector: packet buffer #%d size exceeds limit %d >= %d", i, pb_header->current_pos, packet_buffer_size_actual);
			}

			if (bytes_to_send > 0 && bytes_to_send + pb_header->current_pos >= packet_buffer_size_actual)
			{
				buff->magic   = METRICS_PACKET_MAGIC;
				buff->version = METRICS_PACKET_VERSION;
				buff->pkttype = METRICS_PKTTYPE_BATCH;
				buff->seg_id  = 0;
				buff->len     = bytes_to_send;

				send[batches_to_send] = buff;
				batches_to_send++;

				buff = (batch_packet_header*) palloc0(sizeof(batch_packet_header) + packet_buffer_size_actual + FLUSH_BUFF_GUARD);
				bytes_to_send = 0;
			}

			memcpy(((char*)((buff)+1)) + bytes_to_send, pb_header->head, pb_header->current_pos);
			bytes_to_send += pb_header->current_pos;

			pb_header->current_pos = 0;
			pb_header->current     = pb_header->head;
		}
		clear_merge_operations(pb_header);

		SpinLockRelease(&pb_header->lock);
	}

	if (bytes_to_send > 0)
	{
		buff->magic   = METRICS_PACKET_MAGIC;
		buff->version = METRICS_PACKET_VERSION;
		buff->pkttype = METRICS_PKTTYPE_BATCH;
		buff->seg_id  = 0;
		buff->len     = bytes_to_send;

		send[batches_to_send] = buff;
		batches_to_send++;
	}

	for (i = 0; i < batches_to_send; i++)
	{
		//elog(LOG, "Metrics collector: SEND BATCH PACKET (%d/%d) %d", i+1, batches_to_send, send[i]->len);
		pbuff->main_connected = (send_single_packet(send[i], send[i]->len + sizeof(batch_packet_header)) >= 0);
	}

	MemoryContextSwitchTo(oldcontext);
	MemoryContextReset(flushBuffContext);

	current_bucket += 1;
	if (current_bucket >= NUM_PACKET_BUCKET)
		current_bucket = 0;
}

static bool
ShouldUpdateLocksInfo(int max)
{
	static uint32 numLocks  = 0;
	static uint64 locksHash = 0;

	bool   ret;
	uint32 newNumLocks;
	uint64 newLocksHash;
	int    i;

	ret          = false;
	newNumLocks  = 0;
	newLocksHash = 0;
	for (i       = 0; i < max; i++)
	{
		PgBackendStatus
			*beentry = pgstat_fetch_stat_beentry(i + 1); /* 1-based index */
		if (beentry == NULL)
			continue;
// FIXME:
/*
		if (beentry->st_waiting == PGBE_WAITING_LOCK)
		{
			newNumLocks++;
			newLocksHash += beentry->st_procpid;
		}
*/
	}

	if (newNumLocks != numLocks || newLocksHash != locksHash)
	{
		ret       = true;
		numLocks  = newNumLocks;
		locksHash = newLocksHash;
	}

	return ret;
}

void
InitNodeInstrPkt(metrics_instrument_pack *pkt)
{
	Assert(pkt);
	memset(pkt, 0x00, sizeof(metrics_instrument_pack));
	pkt->magic   = METRICS_PACKET_MAGIC;
	pkt->version = METRICS_PACKET_VERSION;
	pkt->pkttype = METRICS_PKTTYPE_INSTR;
}

/**
 * Sends a UDP packet to agent containing query metrics information.
 */
static void
UpdateNodeInstrPkt(metrics_instrument_pack *pkt)
{
	int                  i;
	InstrumentationSlot  *current;
	Instrumentation      *instr;
	metrics_instrument_t node;

	if (NULL == InstrumentGlobal)
		return;

	Assert(pkt);

	current = (InstrumentationSlot *) (InstrumentGlobal + 1);
	for (i  = 0; i < InstrShmemNumSlots(); i++, current++)
	{
		instr = &current->data;

		/* If this slot is empty, continue to next one */
		if (SlotIsEmpty(current))
			continue;

		if (pkt->length < INSTRUMENT_NUMBER_PER_UDP)
		{
			node.tmid       = current->tmid;
			node.ssid       = current->ssid;
			node.ccnt       = current->ccnt;
			node.segid      = current->segid;
			node.pid        = current->pid;
			node.nid        = (int32) current->nid;
			node.running    = instr->running;
			node.tuplecount = instr->tuplecount;
			node.ntuples    = instr->ntuples;
			node.nloops     = instr->nloops;

			// Strict validate instrument slot
			//   SlotIsEmpty ensure first 8 bytes is not 0xd5d5d5d5
			//   segid == GpIdentity.segindex to ensure this slot is not being erased to 0x00
			//   pid > 0 to double check the slot has valid data
			//   nid > 0 to ensure this slot is correctly initialized
			if(!SlotIsEmpty(current) /*&& node.segid == GpIdentity.segindex*/ && node.pid > 0 && node.nid > 0)
				pkt->instr[pkt->length++] = node;
		}

		if (pkt->length == INSTRUMENT_NUMBER_PER_UDP)
		{
#ifdef MC_DEBUG_PRINT
			ereport(DEBUG4,
			        (errmsg("Metrics collector sent %ld instruments to %d",
			                pkt->length,
			                gpcc_query_metrics_port)));
			for (int j = 0; j < pkt->length; j++)
			{
				metrics_instrument_t n = pkt->u.instr[j];
				ereport(DEBUG4,
				        (errmsg("  INSTR: Batch=%ld S=%d N=%d T=%ld L=%ld",
				                signature,
				                n.segid,
				                n.nid,
				                n.tuplecount,
				                n.nloops)));
			}
#endif // MC_DEBUG_PRINT
			pkt->gp_segment_id = -1;
			pkt->signature = signature;
			send_packet(pkt, sizeof(metrics_instrument_pack), current_bucket, MERGE_NONE);

			current_bucket += 1;
			if (current_bucket >= NUM_PACKET_BUCKET)
				current_bucket = 0;

			InitNodeInstrPkt(pkt);
		}
	}

	if (pkt->length != 0)
	{
#ifdef MC_DEBUG_PRINT
		ereport(DEBUG4,
		        (errmsg("Metrics collector sent %ld instruments to %d",
		                pkt->length,
		                gpcc_query_metrics_port)));
		for (int j = 0; j < pkt->length; j++)
		{
			metrics_instrument_t n = pkt->u.instr[j];
			ereport(DEBUG4,
			        (errmsg("  INSTR: Batch=%ld S=%d N=%d T=%ld L=%ld",
			                signature,
			                n.segid,
			                n.nid,
			                n.tuplecount,
			                n.nloops)));
		}
#endif // MC_DEBUG_PRINT
		pkt->gp_segment_id = -1;
		pkt->signature = signature;
		send_packet(pkt, sizeof(metrics_instrument_t) * pkt->length + 48, current_bucket, MERGE_NONE); // 48 is header part in metrics_instrument_pack

		current_bucket += 1;
		if (current_bucket >= NUM_PACKET_BUCKET)
			current_bucket = 0;

		InitNodeInstrPkt(pkt);
	}
}

void
InitSpillFilePkt(metrics_spill_pack *pkt)
{
	Assert(pkt);
	memset(pkt, 0x00, sizeof(metrics_spill_pack));
	pkt->magic   = METRICS_PACKET_MAGIC;
	pkt->version = METRICS_PACKET_VERSION;
	pkt->pkttype = METRICS_PKTTYPE_SPILLFILE;
}

static void
UpdateSpillFilePkt(metrics_spill_pack *pkt)
{
	metrics_spill_file_t	*sp;
	List					*splist = NIL;

	/**
	 * Clear up any left over memory chunks before proceed with
	 * spill file entries.
	 * This is extra protection observed through MPP-30447 MPP-30545,
	 * If there is any memory corruption inside UpdateSpillFilePkt()
	 * We can locate it precisely.
	 */
	MemoryContextReset(metricsCollectorContext);

	splist = gather_workfile_entries();

	if (splist != NIL)
	{
		int      j = 0;
		ListCell *cell;
		foreach(cell, splist)
		{
			j++;
			sp = lfirst(cell);
#ifdef MC_DEBUG_PRINT
			ereport(DEBUG4,
			        (errmsg(
				        " SpillFile[%d/%d]: Segid=%d Tmid=%d Ssid=%d Ccnt=%d  WorkFileSize=%ld",
				        j,
				        splist->length,
				        GpIdentity.segindex,
				        sp->tmid,
				        sp->sess_id,
				        sp->ccnt,
				        sp->workfile_size)));
#endif // MC_DEBUG_PRINT
			pkt->spill_file[pkt->length++] = *sp;
			if (pkt->length >= SPILL_FILE_NUMBER_PER_UDP)
			{
				pkt->total = splist->length;
				pkt->gp_segment_id = -1;
				pkt->signature = signature;
				send_packet(pkt, sizeof(metrics_spill_pack), current_bucket, MERGE_NONE);

				current_bucket += 1;
				if (current_bucket >= NUM_PACKET_BUCKET)
					current_bucket = 0;

				InitSpillFilePkt(pkt);
			}
		}
		if (pkt->length > 0)
		{
			pkt->total = splist->length;
			pkt->gp_segment_id = -1;
			pkt->signature = signature;
			send_packet(pkt, sizeof(metrics_spill_file_t) * pkt->length + 48, current_bucket, MERGE_NONE); //48 is header part of metrics_spill_pack

			current_bucket += 1;
			if (current_bucket >= NUM_PACKET_BUCKET)
				current_bucket = 0;

			InitSpillFilePkt(pkt);
		}
		foreach(cell, splist)
		{
			pfree(lfirst(cell));
		}
		list_free(splist);
	}

	/**
	 * Clear up memory chunks used for emitting spill file entries.
	 * This is extra protection observed through MPP-30447 MPP-30545,
	 * If there is any memory corruption happened already, we can
	 * locate it precisely before the snowball grows out of control.
	 */
	MemoryContextReset(metricsCollectorContext);
}

/*
 * Alloc shmem space for batch sending buffer
 */
void
init_packet_buff_shmem(void)
{
	Size		size = 0;
	size = add_size(size, sizeof(PacketBufferHeader) * NUM_PACKET_BUCKET);
	size = add_size(size, sizeof(PacketBufferBody) * NUM_PACKET_BUCKET);

	RequestAddinShmemSpace(size);
	prev_packet_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = packet_buff_shmem_startup;
}

/*
 * Initialize shmem space for batch sending buffer
 */
static void
packet_buff_shmem_startup(void)
{
	bool		found;
	int			i;

	if (prev_packet_shmem_startup_hook)
		(*prev_packet_shmem_startup_hook)();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	pbuff = (PacketBuffer*)ShmemInitStruct("mc_packet_buff",
							sizeof(PacketBuffer),
							&found);
	if (!found)
		memset((void*)pbuff, 0x00, sizeof(PacketBuffer));

	pbuff->main_connected = false;
	for (i = 0; i < NUM_PACKET_BUCKET; i++)
	{
		pbuff->h[i].head        = &pbuff->b[i];
		pbuff->h[i].current     = pbuff->h[i].head;
		pbuff->h[i].current_pos = 0;
		SpinLockInit(&pbuff->h[i].lock);
	}

	LWLockRelease(AddinShmemInitLock);
}

void
_PG_fini()
{
	if (metricsCollectorContext)
	{
		MemoryContextDelete(metricsCollectorContext);
		metricsCollectorContext = NULL;
	}
	queryInfo_hook_finishup();
	flushBuffContext_finishup();
}


PG_FUNCTION_INFO_V1(metrics_collector_start_worker);

#define MC_LOOP_SLEEP_MS 100L
#define START_MC_BGWORKER "select gpmetrics.metrics_collector_start_worker();"

extern bool UtilityQuery;

void
EmitUtilityQueryInfo(Node *parsetree,
			const char *queryString,
			//ProcessUtilityContext context,
			ParamListInfo params,
			DestReceiver *dest,
			char *completionTag);
static bool
is_inner_select_query(Node *parsetree);

void
_PG_init(void);

Datum
metrics_collector_start_worker(PG_FUNCTION_ARGS);
static void
metrics_collector_start_worker_internal(bool isDynamic);
static void
metrics_collector_worker_main(Datum arg);
static void
metrics_collector_worker_static_main(Datum arg);
static void
metrics_collector_shmem_startup(void);
//static void
//mc_object_access_hook(ObjectAccessType access, Oid classId, Oid objectId, int subId, void *arg);
static void
init_mc_shmem(void);

// exactly the same as BackgroundWorkerHandle
typedef struct MetricsCollectorHandler
{
	int		slot;
	uint64		generation;
} MetricsCollectorHandler;

static MetricsCollectorHandler *worker_handler;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static LWLock *mc_bg_handler_lock = NULL;
//static object_access_hook_type next_object_access_hook;
//static ProcessUtility_hook_type next_process_utility_hook;

/* flags set by signal handlers */
static volatile sig_atomic_t got_sigterm = false;

/*
 * Signal handler for SIGTERM
 * Set a flag to let the main loop to terminate, and set our latch to wake
 * it up.
 */
static void
mc_worker_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

static void
cleanUtilityFlag(ResourceReleasePhase phase, bool isCommit, bool isTopLevel, void *arg)
{
	if (phase != RESOURCE_RELEASE_AFTER_LOCKS)
		return;

	UtilityQuery = false;
}

/*
 * Entrypoint of this module.
 */
void
_PG_init(void)
{
    /*
     * Skip loading hook function when GP in utility mode
     */
	init_guc();
    /*
     * gp_enable_query_metrics is a cluster level GUC
     * To enable metrics_collector.so, set gp_enable_query_metrics to on
     */
	if (enable_query_metrics)
	{
		query_info_collect_hook = &EmitQueryInfo;
		ereport(LOG, (errmsg("Metrics collector: query info collect hook initialized")));
		//FIXME:
		//next_process_utility_hook = ProcessUtility_hook;
		//ProcessUtility_hook = &EmitUtilityQueryInfo;
		ereport(LOG, (errmsg("Metrics collector: utility query info collect hook initialized")));
		RegisterResourceReleaseCallback(cleanUtilityFlag, NULL);

		/* Add mc_object_access_hook to handle drop extension event.*/
		//FIXME:
		//next_object_access_hook = object_access_hook;
		//object_access_hook = mc_object_access_hook;

		// Request shared memory
		init_mc_shmem();
		init_packet_buff_shmem();


			// prepare metrics collector error hooks
		init_mc_error_data_hook();

		// On master's QD only, prepare directories for query text file
		// Skip this for utility connection on segment
	
			/*
				* Start a static bgworker when gpdb starts
				* 1. Create a dynamic bgworker to collect cluster metrics, If metrics collector extension already exists.
				* 2. Do nothing if metrics collector extension does not exist.
				*/
		metrics_collector_start_worker_internal(false);

		char queryTextFilePath[MAXPGPATH];
		struct stat st;

		if (stat(DIR_GPCC_METRICS, &st) < 0 && mkdir(DIR_GPCC_METRICS, S_IRWXU))
			ereport(LOG, (errmsg("Metrics collector: failed to create gpmetrics directory %s", DIR_GPCC_METRICS)));

		join_path_components(queryTextFilePath, DIR_GPCC_METRICS, DIR_QUERY_TEXT);
		canonicalize_path(queryTextFilePath);

		if (stat(queryTextFilePath, &st) < 0 && mkdir(queryTextFilePath, S_IRWXU))
			ereport(LOG, (errmsg("Metrics collector: failed to create query_text directory %s", queryTextFilePath)));
			
	}
}

// Request shared memory slot and a LWLock
static void 
init_mc_shmem(void)
{
	//RequestAddinLWLocks(1);
	RequestAddinShmemSpace(sizeof(MetricsCollectorHandler));
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = metrics_collector_shmem_startup;
}

// Initialize MetricsCollectorHandler struct for bgworker in shared memory
static void 
metrics_collector_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		(*prev_shmem_startup_hook)();
	

	//mc_bg_handler_lock = LWLockAssign();
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	
	worker_handler = ShmemInitStruct("mc_bgworker_handler",
							sizeof(MetricsCollectorHandler),
							&found);
	if (!found)
		memset((void*)worker_handler, 0, sizeof(MetricsCollectorHandler));

	LWLockRelease(AddinShmemInitLock);
}

// Main function for metrics collector launcher process
static void
metrics_collector_worker_static_main(Datum arg)
{
	BackgroundWorkerUnblockSignals();
	
	BackgroundWorkerInitializeConnection("gpperfmon", NULL, 0);
	
	StartTransactionCommand();
	// if extension already exists, start a dynamic bgworker on master and all segments
	// if not, do nothing and exit
	if(get_extension_oid("metrics_collector", true) != InvalidOid)
	{
		ereport(LOG, (errmsg("Metrics collector: try to start background worker because extension exists.")));
		if(enable_query_metrics)
		{
			metrics_collector_start_worker_internal(true);
			//dispatch_udf_to_segDBs(START_MC_BGWORKER);
		}
		else
			ereport(LOG, (errmsg("Metrics collector: skip start background worker because enable_query_metrics is off")));
	}
	CommitTransactionCommand();
	exit(0);
}

// Main function for metrics collector worker process
static void
metrics_collector_worker_main(Datum arg)
{
	int rc;
	pqsignal(SIGTERM, mc_worker_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();
	
	/* Create and initialize metrics_packet */
	InitNodeInstrPkt(&nodeinfopkt);
	InitLockInfoPkt(&lockinfopkt);
	InitStatActivityPkt(&activitypkt);
	InitSpillFilePkt(&spillfilepkt);

	metricsCollectorContext = AllocSetContextCreate(TopMemoryContext,
	                                         "MetricsCollectorMemCtxt",
	                                         ALLOCSET_DEFAULT_INITSIZE,
	                                         ALLOCSET_DEFAULT_INITSIZE,
	                                         ALLOCSET_DEFAULT_MAXSIZE);

	ereport(LOG, (errmsg("Metrics collector: background worker starts")));
	
	while(!got_sigterm)
	{
		CHECK_FOR_INTERRUPTS();

		/* no need to live on if postmaster has died */
		if (!PostmasterIsAlive())
			exit(1);

		MetricsCollectorLoopFunc();

		/* Sleep a while. */
		//FIXME: sleep time
		rc = WaitLatch(&MyProc->procLatch,
				WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
				MC_LOOP_SLEEP_MS, WAIT_EVENT_PG_SLEEP);
		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}	
	/* gp will restart it if exit code is not 0. */
	proc_exit(1);
}

/*
 * Function starting metrics collector worker
 */
Datum
metrics_collector_start_worker(PG_FUNCTION_ARGS)
{
	if (!enable_query_metrics)
		ereport(ERROR, 
				(errmsg("Metrics collector: failed to create metrics_collector extension"),
				 errhint("Make sure enable_query_metrics GUC turned on")));

	metrics_collector_start_worker_internal(true);


	PG_RETURN_VOID();
}

/*
 * Main function for start a static or dynamic background worker
 */
static void
metrics_collector_start_worker_internal(bool isDynamic)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	MemoryContext old_ctx;
	pid_t pid;
	BgwHandleStatus status = BGWH_STOPPED;

	memset(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;

	if (isDynamic)
	{
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
		worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
		snprintf(worker.bgw_name, sizeof(worker.bgw_name), "%s", "metrics collector");
		sprintf(worker.bgw_function_name, "metrics_collector_worker_main");
		/* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
		worker.bgw_notify_pid = MyProcPid;
		worker.bgw_main_arg = (Datum) 0;

		old_ctx = MemoryContextSwitchTo(TopMemoryContext);
		if (!RegisterDynamicBackgroundWorker(&worker, &handle))
			ereport(ERROR, (errmsg("Metrics collector: register dynamic background worker failed")));

		MemoryContextSwitchTo(old_ctx);

		status = WaitForBackgroundWorkerStartup(handle, &pid);
		if (status == BGWH_STOPPED)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					errmsg("Metrics collector: could not start background process"),
					errhint("More details may be available in the server log.")));
		if (status == BGWH_POSTMASTER_DIED)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					errmsg("Metrics collector: cannot start background processes without postmaster"),
					errhint("Kill all remaining database processes and restart the database.")));

		Assert(status == BGWH_STARTED);

		LWLockAcquire(mc_bg_handler_lock, LW_EXCLUSIVE);
		memcpy(worker_handler, handle, sizeof(MetricsCollectorHandler));
		LWLockRelease(mc_bg_handler_lock);
	}
	else
	{
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
			BGWORKER_BACKEND_DATABASE_CONNECTION;
		worker.bgw_restart_time = BGW_NEVER_RESTART;
		snprintf(worker.bgw_name, sizeof(worker.bgw_name), "%s", "metrics collector launcher");
		sprintf(worker.bgw_function_name, "metrics_collector_worker_static_main");
		RegisterBackgroundWorker(&worker);
	}
}

/*
 * mc_object_access_hook is to stop bgworker when drop extension
 */
/*FIXME:
static void
mc_object_access_hook(ObjectAccessType access, Oid classId,
			Oid objectId, int subId, void *arg)
{
	Oid oid;

	if (next_object_access_hook)
		(*next_object_access_hook)(access, classId, objectId, subId, arg);

	if (access != OAT_DROP || classId != ExtensionRelationId)
		return;

	oid = get_extension_oid("metrics_collector", true);
	if (oid == objectId)
	{
		ereport(LOG, (errmsg("Metrics collector: terminate bgworker")));
		LWLockAcquire(mc_bg_handler_lock, LW_EXCLUSIVE);
		TerminateBackgroundWorker((BackgroundWorkerHandle *)worker_handler);
		memset(worker_handler, 0x7f, sizeof(MetricsCollectorHandler));
		LWLockRelease(mc_bg_handler_lock);
	}
}*/

/*
 * If ProcessUtility_hook is already set by another user, call it.
 * Otherwise, call standard_ProcessUtility.
 */
// FIXME:
/*
void
AdaptorProcessUtility(Node *parsetree,
			const char *queryString,
			ProcessUtilityContext context,
			ParamListInfo params,
			DestReceiver *dest,
			char *completionTag)
{
	if (next_process_utility_hook)
		(*next_process_utility_hook)(parsetree, queryString,
								context, params,
								dest, completionTag);
	else
		standard_ProcessUtility(parsetree, queryString,
								context, params,
								dest, completionTag);
}*/

/*
 * Utility hook function to emit utility query information.
 * send METRICS_QUERY_SUBMIT&METRICS_QUERY_START before standard_ProcessUtility,
 * send METRICS_QUERY_DONE after it.
 */
//FIXME: EmitUtilityQueryInfo

/*
 * Emit query guc, do this when status is METRICS_QUERY_SUBMIT
 */
// FIXME: emit_query_guc

/*
 * Judge whether the query will increase gp_command_count more than once
 * by executing select in it.
 * For example:
 *   COPY (SELECT * FROM ... ) TO ''
 *   CREATE TABLE AS SELECT * FROM ...
 * For these query, skip ProcessUtility_hook and catch them by
 * query_info_collect_hook. Otherwise, frontend will see a same query
 * which ccnt is largger from Activity.
 */
static bool
is_inner_select_query(Node *parsetree)
{
	if (parsetree == NULL)
	{
		return false;
	}
	else if (parsetree->type == T_CopyStmt)
	{
		List *next = (List*)((List*)parsetree)->tail;
		if (next != NULL && next->type == T_SelectStmt)
		{
			return true;
		}
	}
	else if (parsetree->type == T_CreateTableAsStmt)
	{
		return true;
	}
	return false;
}
