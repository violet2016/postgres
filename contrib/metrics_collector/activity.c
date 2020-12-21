/*-------------------------------------------------------------------------
 *
 * activity.c
 * Utility function for pg_stat_activity
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "activity.h"
#include "metrics_collector.h"
#include "utils_pg.h"
void
InitStatActivityPkt(metrics_activity_t *pkt)
{
	Assert(pkt);
	memset(pkt, 0x00, sizeof(metrics_activity_t));
	pkt->magic   = METRICS_PACKET_MAGIC;
	pkt->version = METRICS_PACKET_VERSION;
	pkt->pkttype = METRICS_PKTTYPE_ACTIVITY;
}

/**
 * Sends a UDP packet to agent containing query metrics information.
 */
void
UpdateStatActivityPkt(metrics_activity_t *pkt, int maxLoop)
{
	uint32              i;
	struct timeval      tv;
	TIMESTAMP           time;
	PGPROC             *proc;
	PgBackendStatus    *beentry;
	size_t              query_text_len;

	Assert(pkt);

	gettimeofday(&tv, 0);
	time = ((TIMESTAMP) tv.tv_sec) + ((TIMESTAMP) tv.tv_usec) / 1000000.0;

	if (maxLoop == 0)
	{
#ifdef MC_DEBUG_PRINT
		ereport(DEBUG4, (errmsg("Activity[0/0]")));
#endif // MC_DEBUG_PRINT

		pkt->total         = maxLoop;
		pkt->timestamp     = time;
		pkt->gp_segment_id = -1;
		pkt->signature     = signature;
		send_packet(pkt, sizeof(metrics_activity_t), current_bucket, MERGE_NONE);

		current_bucket += 1;
		if (current_bucket >= NUM_PACKET_BUCKET)
			current_bucket = 0;

		InitStatActivityPkt(pkt);
	}
	else
	{
		for (i = 0; i < maxLoop; i++)
		{
			beentry = pgstat_fetch_stat_beentry(i + 1); /* 1-based index */

			Assert(beentry);

			//pkt->tmid      = tmid;
			pkt->changecnt = beentry->st_changecount;
			pkt->datid     = beentry->st_databaseid;
			pkt->procpid   = beentry->st_procpid;
			pkt->usesysid  = beentry->st_userid;
			//pkt->waiting   = beentry->st_waiting;
			//pkt->sess_id   = beentry->st_session_id;
			pkt->ccnt      = (-1);

			/* Only on master */
			if (beentry->st_procpid > 0)
			{
				pkt->query_start   = TIMESTAMPTZ_TO_DOUBLE(beentry->st_activity_start_timestamp);
                pkt->act_start     = TIMESTAMPTZ_TO_DOUBLE(beentry->st_xact_start_timestamp);
                pkt->backend_start = TIMESTAMPTZ_TO_DOUBLE(beentry->st_proc_start_timestamp);
                snprintf(pkt->application_name, NAMEDATALEN, "%s",
                            beentry->st_appname ? beentry->st_appname : "");
                // FIXME:
                //query_text_len = snprintf(pkt->current_query, STAT_ACTIVITY_QUERY_SIZE, "%s",
                //            beentry->st_activity ? beentry->st_activity : "") + 1;
                pkt->is_query_text_complete = (query_text_len < STAT_ACTIVITY_QUERY_SIZE && query_text_len < pgstat_track_activity_query_size) ? 't' : 'f';

                proc = BackendPidGetProc(beentry->st_procpid);
                // FIXME:
                /*
                if (proc != NULL)
                    pkt->ccnt = proc->queryCommandId;
				*/
			}
			pkt->state = GPCC_STATE_UNKNOWN;
			populate_act_state(pkt, beentry);

#ifdef MC_DEBUG_PRINT
			ereport(DEBUG4,
			        (errmsg(
				        "Activity[%d/%ld]: B=%ld S=%d P=%d SS=%d CC=%d A=%s D=%d U=%d Q=%s Tt=%d St=%d Bt=%d",
				        i + 1,
				        pkt->total,
				        signature,
				        GpIdentity.segindex,
				        pkt->procpid,
				        pkt->sess_id,
				        pkt->ccnt,
				        pkt->application_name,
				        pkt->datid,
				        pkt->usesysid,
				        pkt->current_query,
				        (int32)pkt->act_start,
				        (int32)pkt->query_start,
				        (int32)pkt->backend_start)));
#endif // MC_DEBUG_PRINT

			pkt->total     = maxLoop;
			pkt->timestamp = time;
			pkt->gp_segment_id = -1;
			pkt->signature = signature;
			send_packet(pkt, sizeof(metrics_activity_t), current_bucket, MERGE_NONE);

			current_bucket += 1;
			if (current_bucket >= NUM_PACKET_BUCKET)
				current_bucket = 0;

			InitStatActivityPkt(pkt);
		}
	}
}
