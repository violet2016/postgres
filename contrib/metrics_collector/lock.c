/*-------------------------------------------------------------------------
 *
 * lock.c
 * Utility function for locks
 *
 * Copyright (c) 2018-Present Pivotal Software, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "metrics_collector.h"
#include "lock.h"
#include "utils_pg.h"
void
InitLockInfoPkt(metrics_lock_pack *pkt)
{
	Assert(pkt);
	memset(pkt, 0x00, sizeof(metrics_lock_pack));
	pkt->magic   = METRICS_PACKET_MAGIC;
	pkt->version = METRICS_PACKET_VERSION;
	pkt->pkttype = METRICS_PKTTYPE_LOCK;
}

void
UpdateLockInfoPkt(metrics_lock_pack *pkt)
{
	int             i;
	int             j = 0;
	struct timeval  tv;
	TIMESTAMP       time;
	LockData       *lockData;
	List           *llist = NIL;
	metrics_lock_t *lr = NULL;
	ListCell       *cell;
	PGPROC         *proc;
	List           *flList = NIL;

	/**
	 * Clear up any left over memory chunks before proceed with
	 * lock status data.
	 * This is extra protection observed through MPP-30447 MPP-30545,
	 * If there is any memory corruption inside UpdateLockInfoPkt()
	 * We can locate it precisely.
	 */
	MemoryContextReset(metricsCollectorContext);

	Assert(pkt);

	lockData = GetLockStatusData();

	if (lockData != NULL)
	{
		gettimeofday(&tv, 0);
		time = ((TIMESTAMP) tv.tv_sec) + ((TIMESTAMP) tv.tv_usec) / 1000000.0;

		for (i = 0; i < lockData->nelements;)
		{
            // FIXME: tmid
			lr = populate_lock_data(lockData, &i, 0);
			if (lr != NULL)
				llist = lappend(llist, lr);
		}

		if (llist)
		{
			flList = LockFilter(llist);
			foreach(cell, flList)
			{
				j++;
				lr = lfirst(cell);
				if (lr->pid > 0 && lr->queryCommandId == -1)
				{
					proc = BackendPidGetProc(lr->pid);
                    /*
					if (proc != NULL)
						lr->queryCommandId = proc->queryCommandId;
                    */
				}

#ifdef MC_DEBUG_PRINT
				ereport(DEBUG4,
				        (errmsg(
					        " LOCK[%d/%d]: Batch=%ld P=%d T=%d M=%d D=%d R=%d S=%d Q=%d-%d-%d",
					        j,
					        flList->length,
					        signature,
					        lr->pid,
					        lr->cclocktype,
					        lr->mode,
					        lr->database,
					        lr->relation,
					        GpIdentity.segindex,
					        tmid,
					        lr->mppSessionId,
					        lr->queryCommandId)));
#endif // MC_DEBUG_PRINT

				pkt->lock[pkt->length++] = *lr;
				if (pkt->length >= LOCKS_NUMBER_UDP)
				{
					pkt->total     = flList->length;
					pkt->timestamp = time;
					pkt->gp_segment_id = -1;
					pkt->signature = signature;
					send_packet(pkt, sizeof(metrics_lock_pack), current_bucket, MERGE_NONE);

					current_bucket += 1;
					if (current_bucket >= NUM_PACKET_BUCKET)
						current_bucket = 0;

					InitLockInfoPkt(pkt);
				}
			}
			// Emit empty packet if no locks
			if (pkt->length > 0 || (pkt->length == 0 && !flList))
			{
				if (flList)
				{
					pkt->total     = flList->length;
				}
				pkt->timestamp = time;
				pkt->gp_segment_id = -1;
				pkt->signature = signature;
				send_packet(pkt, sizeof(metrics_lock_t) * pkt->length + 48, current_bucket, MERGE_NONE); //48 is header part in metrics_lock_pack

				current_bucket += 1;
				if (current_bucket >= NUM_PACKET_BUCKET)
					current_bucket = 0;

				InitLockInfoPkt(pkt);
			}
			foreach(cell, llist)
			{
				metrics_lock_t *lr = lfirst(cell);
				pfree(lr);
			}
			list_free(llist);
			list_free(flList);
		}
	}

	/**
	 * Clear up memory chunks used for processing locks data.
	 * This is extra protection observed through MPP-30447 MPP-30545,
	 * If there is any memory corruption happened already, we can
	 * locate it precisely before the snowball grows out of control.
	 */
	MemoryContextReset(metricsCollectorContext);
}

void
populate_lock_tag(metrics_lock_t *lr, LOCKTAG *tag)
{
	SET_GPCC_LOCKTAG(lr, tag->locktag_type);
	switch (lr->cclocktype)
	{
		case GPCC_LOCKTAG_RELATION:
		case GPCC_LOCKTAG_RELATION_EXTEND:
		case GPCC_LOCKTAG_RELATION_RESYNCHRONIZE:
			lr->database = tag->locktag_field1;
			lr->relation = tag->locktag_field2;
			break;
		case GPCC_LOCKTAG_PAGE:
			lr->database = tag->locktag_field1;
			lr->relation = tag->locktag_field2;
			lr->page     = tag->locktag_field3;
			break;
		case GPCC_LOCKTAG_TUPLE:
			lr->database = tag->locktag_field1;
			lr->relation = tag->locktag_field2;
			lr->page     = tag->locktag_field3;
			lr->tuple    = tag->locktag_field4;
			break;
		case GPCC_LOCKTAG_TRANSACTION:
			lr->transactionid = tag->locktag_field1;
			break;
		case GPCC_LOCKTAG_VIRTUALTRANSACTION:
			lr->xbackendid          = tag->locktag_field1,
			lr->xlocaltransactionid = tag->locktag_field2;
			break;
		case GPCC_LOCKTAG_RELATION_APPENDONLY_SEGMENT_FILE:
			lr->database = tag->locktag_field1;
			lr->relation = tag->locktag_field2;
			lr->classid  = tag->locktag_field3;
			break;
		case GPCC_LOCKTAG_RESOURCE_QUEUE:
#if 0 // databaseId is no use in GPCC, so it's safe to ignore this field
			lr->database = proc->databaseId;
#endif
			lr->objid    = tag->locktag_field1;
			break;
		case GPCC_LOCKTAG_OBJECT:
		case GPCC_LOCKTAG_USERLOCK:
		case GPCC_LOCKTAG_ADVISORY:
		default:            /* treat unknown locktags like OBJECT */
			lr->database = tag->locktag_field1;
			lr->classid  = tag->locktag_field2;
			lr->objid    = tag->locktag_field3;
			lr->objsubid = tag->locktag_field4;
			break;
	}
}

List*
LockFilter(List* lList)
{
	List *txList = NULL;
	List *reList = NULL;
	List *vxList = NULL;
	List *flList = NULL;

	ListCell *cell;
	foreach(cell, lList)
	{
		metrics_lock_t *lr = lfirst(cell);
		if (lr->granted)
		{
			continue;
		}
		switch(lr->cclocktype)
		{
			case GPCC_LOCKTAG_TRANSACTION:
				txList = lappend(txList, lr);
				break;
			case GPCC_LOCKTAG_VIRTUALTRANSACTION:
				vxList = lappend(vxList, lr);
				break;
			case GPCC_LOCKTAG_RELATION:
			case GPCC_LOCKTAG_RELATION_EXTEND:
			case GPCC_LOCKTAG_PAGE:
			case GPCC_LOCKTAG_TUPLE:
			case GPCC_LOCKTAG_RELATION_RESYNCHRONIZE:
			case GPCC_LOCKTAG_RELATION_APPENDONLY_SEGMENT_FILE:
				reList = lappend(reList, lr);
				break;
		}
	}
	if(!txList && !reList && !vxList)
	{
		return NULL;
	}

	foreach(cell, lList)
	{
		metrics_lock_t *lr = lfirst(cell);
		ListCell *lockCell = NULL;
		switch(lr->cclocktype)
		{
			case GPCC_LOCKTAG_TRANSACTION:
				foreach(lockCell, txList)
				{
					metrics_lock_t *txLock = lfirst(lockCell);
					if (txLock->transactionid == lr->transactionid)
					{
						flList = lappend(flList, lr);
						break;
					}
				}
				break;
			case GPCC_LOCKTAG_VIRTUALTRANSACTION:
				foreach(lockCell, vxList)
				{
					metrics_lock_t *vxLock = lfirst(lockCell);
					if (vxLock->xbackendid == lr->xbackendid && vxLock->xlocaltransactionid == lr->xlocaltransactionid)
					{
						flList = lappend(flList, lr);
						break;
					}
				}
				break;
			case GPCC_LOCKTAG_RELATION:
			case GPCC_LOCKTAG_RELATION_EXTEND:
			case GPCC_LOCKTAG_PAGE:
			case GPCC_LOCKTAG_TUPLE:
			case GPCC_LOCKTAG_RELATION_RESYNCHRONIZE:
			case GPCC_LOCKTAG_RELATION_APPENDONLY_SEGMENT_FILE:
				foreach(lockCell, reList)
				{
					metrics_lock_t *reLock = lfirst(lockCell);
					if (reLock->relation == lr->relation)
					{
						flList = lappend(flList, lr);
						break;
					}
				}
				break;
			default:
				flList = lappend(flList, lr);
				break;
		}
	}
	return flList;
}
