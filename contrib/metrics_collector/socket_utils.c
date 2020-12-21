/*-------------------------------------------------------------------------
 *
 * metrics_utils.c
 *    Util functions for sending udp packets
 *
 * Copyright (c) 2018-Present Pivotal Software, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "socket_utils.h"
#include "utils_pg.h"
static int
try_add_to_packet_buffer(void *p, size_t n, int bucket, int save_merge);
#ifdef PACKET_TRACE
static void
packet_trace(void *p);
#endif
static metrics_conn conn = {-1};

#if defined(__linux__)
	#define METRICS_NAME "/tmp/.s.GPMC.%d.sock"
#else
	#define METRICS_SERVER_NAME "/tmp/.s.GPMC.server.sock"
	#define METRICS_CLIENT_NAME "/tmp/.s.GPMC.client.sock"
#endif

MemoryContext flushBuffContext = NULL;
batch_packet_header *flushBuff = NULL;
void
flushBuffContext_finishup(void)
{
	if (flushBuffContext)
	{
		MemoryContextDelete(flushBuffContext);
		flushBuffContext = NULL;
	}
}

int packet_buffer_size_actual = PACKET_BUFFER_SIZE;
int packet_buffer_size_red_line_actual = PACKET_BUFFER_RED_LINE;

/*
 * Check if should initialize connection
 */
bool
should_init_socket(void)
{
#if defined(__linux__)
	if (conn.sock >= 0)
		return false;

	return true;
#else
	return conn.sock < 0 || conn.addr.sin_port != htons(gpcc_query_metrics_port);
#endif
}

int
socket_init(void)
{
	int   sock;
	close(conn.sock);
	conn.sock = -1;
#if defined(__linux__)
	struct sockaddr_un metrics_server;
	sock = socket(AF_UNIX, SOCK_SEQPACKET, 0);
	if (sock >= 0)
	{
		metrics_server.sun_family = AF_UNIX;
		snprintf(metrics_server.sun_path, sizeof(metrics_server.sun_path),
			METRICS_NAME, PostPortNumber);
		if (connect(sock, (struct sockaddr *) &metrics_server, sizeof(struct sockaddr_un)))
		{
			close(sock);
			sock = -1;
			// TODO packet loss action
		}
	}
#else
	sock = socket(AF_INET, SOCK_DGRAM, 0);
	if (sock == -1)
		elog(WARNING, "Metrics collector: cannot create socket");
	if (fcntl(sock, F_SETFL, O_NONBLOCK) == -1)
		elog(WARNING, "Metrics collector: fcntl(F_SETFL, O_NONBLOCK) failed");
	if (fcntl(sock, F_SETFD, 1) == -1)
		elog(WARNING, "Metrics collector: fcntl(F_SETFD) failed");
	memset(&conn.addr, 0, sizeof(conn.addr));
	conn.addr.sin_family      = AF_INET;
	conn.addr.sin_addr.s_addr = inet_addr("127.0.0.1");
	conn.addr.sin_port        = htons(gpcc_query_metrics_port);
#endif

	conn.sock = sock;
	return sock;
}

void
reset_conn(void)
{
	close(conn.sock);
	conn.sock = -1;
	memset(&conn.addr, 0, sizeof(conn.addr));
}

static metrics_query_info *saved_query_info;
static int merge_lsn;

int
try_add_to_packet_buffer(void *p, size_t n, int bucket, int save_merge)
{
	PacketBufferHeader *pb_header;
	int					ret = 0;
	int					n_in_batch = n - SIZE_PACKET_MAGIC_AND_VERSION;

	if (n <= SIZE_PACKET_MAGIC_AND_VERSION)
	{
		// Protection code for invalid packet
		return 0;
	}

	if (n > SINGLE_PACKET_SIZE_LIMIT)
	{
		// Protection code for handling XLarge packet
		send_single_packet(p, n);
		return 0;
	}

	pb_header = &pbuff->h[bucket];

	SpinLockAcquire(&pb_header->lock);

	// Append packet to buff
	//elog(LOG, "Metrics collector: ADD %d TO BATCH", (int32)n_in_batch);
	*((int32*)(pb_header->current)) = n_in_batch;
	pb_header->current = ((int32*)(pb_header->current)) + 1;
	pb_header->current_pos += sizeof(int32);

	if (save_merge == MERGE_QUERY_INFO)
	{
		saved_query_info = (metrics_query_info*)((char*)(pb_header->current) - SIZE_PACKET_MAGIC_AND_VERSION);
		merge_lsn = pb_header->snum;
	}
	memcpy(((char*)(pb_header->head)) + pb_header->current_pos, ((char*)(p)) + SIZE_PACKET_MAGIC_AND_VERSION, n_in_batch);
	pb_header->current = ((char*)(pb_header->current)) + n_in_batch;
	pb_header->current_pos += n_in_batch;

	if (pb_header->current_pos > packet_buffer_size_red_line_actual)
	{
		// Force flush if buffer is about to full
		if (flushBuffContext == NULL)
			flushBuffContext = AllocSetContextCreate(TopMemoryContext,
			                                         "MetricsFlushBuffMemCtxt",
			                                         ALLOCSET_DEFAULT_INITSIZE,
			                                         ALLOCSET_DEFAULT_INITSIZE,
			                                         ALLOCSET_DEFAULT_MAXSIZE);

		MemoryContext oldcontext = MemoryContextSwitchTo(flushBuffContext);
		flushBuff = (batch_packet_header*) palloc0(pb_header->current_pos + sizeof(batch_packet_header) + FLUSH_BUFF_GUARD);
		memcpy(flushBuff+1, pb_header->head, pb_header->current_pos);
		MemoryContextSwitchTo(oldcontext);

		ret = pb_header->current_pos;

		pb_header->current_pos   = 0;
		pb_header->current       = pb_header->head;

		clear_merge_operations(pb_header);
	}

	SpinLockRelease(&pb_header->lock);

	return ret;
}

void
clear_merge_operations(PacketBufferHeader *pb_header)
{
	// Clear all merge operations
	pb_header->snum++;
	if (pb_header->snum > 8192)
		pb_header->snum = 1;
}

void
merge_qlog_packet(metrics_query_info *pkt)
{
	PacketBufferHeader *pb_header;
	int bucket = pkt->qid.ssid & HASH_TO_BUCKET;

	pb_header = &pbuff->h[bucket];

	SpinLockAcquire(&pb_header->lock);

	if (saved_query_info == NULL)
	{
		SpinLockRelease(&pb_header->lock);
		send_packet(pkt, query_info_bytes_to_send(pkt), bucket, MERGE_QUERY_INFO);
		return;
	}
	if (merge_lsn != pb_header->snum)
	{
		SpinLockRelease(&pb_header->lock);
		send_packet(pkt, query_info_bytes_to_send(pkt), bucket, MERGE_QUERY_INFO);
		return;
	}
	if (pkt->qid.tmid != saved_query_info->qid.tmid ||
	    pkt->qid.ssid != saved_query_info->qid.ssid ||
	    pkt->qid.ccnt != saved_query_info->qid.ccnt)
	{
		SpinLockRelease(&pb_header->lock);
		send_packet(pkt, query_info_bytes_to_send(pkt), bucket, MERGE_QUERY_INFO);
		return;
	}

	saved_query_info->status |= pkt->status;
	switch (pkt->status)
	{
	case METRICS_QUERY_STATUS_SUBMIT:
		saved_query_info->tsubmit = pkt->tsubmit;
		break;
	case METRICS_QUERY_STATUS_START:
		saved_query_info->tstart = pkt->tstart;
		break;
	case METRICS_QUERY_STATUS_DONE:
	case METRICS_QUERY_STATUS_CANCELED:
		saved_query_info->tfinish = pkt->tfinish;
		break;
	default:
		break;
	}

	SpinLockRelease(&pb_header->lock);
}

void
send_packet(void *p, size_t n, int bucket, int save_merge)
{
	int    bytes_to_send = 0;

	bytes_to_send = try_add_to_packet_buffer(p, n, bucket, save_merge);
	if (bytes_to_send == 0)
		return;

	if (flushBuff == NULL)
	{
		elog(WARNING, "Metrics collector: flush buffer not allocated");
		return;
	}

	// Send batch packets
	MemoryContext oldcontext = MemoryContextSwitchTo(flushBuffContext);

	flushBuff->magic   = METRICS_PACKET_MAGIC;
	flushBuff->version = METRICS_PACKET_VERSION;
	flushBuff->pkttype = METRICS_PKTTYPE_BATCH;
	flushBuff->seg_id  = 0;
	flushBuff->len     = bytes_to_send;

	//elog(LOG, "Metrics collector: RUSH BATCH PACKET %d", bytes_to_send);
	send_single_packet(flushBuff, bytes_to_send + sizeof(batch_packet_header));
	pfree(flushBuff);
	flushBuff = NULL;

	MemoryContextSwitchTo(oldcontext);
	MemoryContextReset(flushBuffContext);
}

int
send_single_packet(void *p, size_t n)
{
	if (should_init_socket())
		socket_init();

	if (conn.sock >= 0)
	{
#if defined(__linux__)
		int retry = 0;
		while (n != write(conn.sock, p, n))
		{
			if (errno == EINTR)
			{
				//elog(LOG, "Metrics collector: EINTR Send again %d", errno);
				// Send again until successful or other error occurred
				continue;
			}

			// Limit the number of retry
			if (retry == 0)
				retry++;
			else
			{
#ifdef PACKET_TRACE
				packet_trace(p);
#endif // PACKET_TRACE
				break;
			}

			if (errno == EAGAIN)
			{
				//elog(LOG, "Metrics collector: EAGAIN Send again %d %d", errno, retry);
				continue;
			}

			if (errno == EMSGSIZE)
			{
				elog(LOG, "Metrics collector: cannot send packet, size %lu larger than the supported maximum size", n);

#if 0 /* No more buffer size shrinking since MPP-30545 */
				int adjust_size = packet_buffer_size_actual + PACKET_BUFFER_HEAD;
				if (adjust_size > 16384 && n < adjust_size) // Minimum packet size to 16K
				{
					adjust_size = adjust_size >> 1;
					packet_buffer_size_actual = adjust_size - PACKET_BUFFER_HEAD;
					packet_buffer_size_red_line_actual = packet_buffer_size_actual - SINGLE_PACKET_SIZE_LIMIT;
					elog(LOG, "Metrics collector: reduce maximum packet size to %d", packet_buffer_size_actual);
					//FIXME: break into smaller packets and send again
				}
#endif
			}

			//elog(LOG, "Metrics collector: cannot send packet, error %d %d", errno, retry);

			socket_init();
			if (conn.sock >= 0)
			{
				//elog(LOG, "Metrics collector: BATCH: socket_init OK %d", conn.sock);
				continue;
			}
			else
			{
				elog(LOG, "Metrics collector: socket_init failed %d", errno);
#ifdef PACKET_TRACE
				packet_trace(p);
#endif // PACKET_TRACE
				break;
			}
		}
#else // defined(__linux__)
		int send = sendto(conn.sock, p, n, 0,
		                  (struct sockaddr *) &conn.addr, sizeof(conn.addr));
		if (n != send) {
			int err = errno;
			elog(WARNING, "Metrics collector: cannot send packet, (socket %d) (err %d) (Expect %ld, Send %d)", conn.sock, err, n, send);
		}
#ifdef PACKET_TRACE
		else
			packet_trace(p);
#endif // PACKET_TRACE
#endif // defined(__linux__)
	}
	return conn.sock;
}

#ifdef PACKET_TRACE
/*
 * Packet tracing for metrics_collector
 * With this enabled,
 *   touch /tmp/cc_packet_loss_tracing will enable tracking number of packets
 *   rm /tmp/cc_packet_loss_tracing will stop and reset the counters
 * It borrows one slot from Instrument shmem, don't turn on this for production build
 */
typedef struct packet_trace_counter
{
	int64 magic;
	int64 qlog;
	int64 qtext;
	int64 node;
	int64 lock;
	int64 spill;
	int64 instr;
	int64 act;
	int64 others;
} packet_trace_counter;

static void
packet_trace(void *p)
{
	const int64 magic = 45651345134;
	static bool shouldCount = false;
	packet_trace_counter *cnt;
	int32 pktType = *((int32 *)p + 2);

	if (NULL == InstrumentGlobal)
		return;

	Size numSlots = InstrShmemNumSlots();
	if (numSlots <= 0)
		return;

	// Last slot
	InstrumentationSlot *slot = ((InstrumentationSlot *) (InstrumentGlobal + 1)) + (numSlots - 1);
	cnt = (packet_trace_counter *)slot;

	if (cnt->magic != magic)
	{
		memset(cnt, 0x00, sizeof(InstrumentationSlot));
		cnt->magic = magic;
	}

	if( access( "/tmp/cc_packet_loss_tracing", F_OK ) == -1 )
	{
		if (shouldCount == true)
		{
			elog(LOG, "Total Number of packet loss %ld, Qlog %ld, Qtext %ld, Node %ld, Lock %ld, Act %ld, Spill %ld Instr %ld, Others %ld",
				cnt->qlog + cnt->qtext + cnt->node + cnt->lock + cnt->act + cnt->spill + cnt->instr + cnt->others,
				cnt->qlog, cnt->qtext, cnt->node, cnt->lock, cnt->act, cnt->spill, cnt->instr, cnt->others);
			memset(cnt, 0x00, sizeof(InstrumentationSlot));
		}
		shouldCount = false;
		return;
	}

	shouldCount = true;
	SpinLockAcquire(&InstrumentGlobal->lock);
	switch (pktType)
	{
	case METRICS_PKTTYPE_QUERY:
		cnt->qlog++;
		break;
	case METRICS_PKTTYPE_QUERY_TEXT:
		cnt->qtext++;
		break;
	case METRICS_PKTTYPE_INIT_NODE:
	case METRICS_PKTTYPE_INIT_NODE_WITH_CONDITION:
		cnt->node++;
		break;
	case METRICS_PKTTYPE_INSTR:
		cnt->instr++;
		break;
	case METRICS_PKTTYPE_LOCK:
		cnt->lock++;
		break;
	case METRICS_PKTTYPE_ACTIVITY:
		cnt->act++;
		break;
	case METRICS_PKTTYPE_SPILLFILE:
		cnt->spill++;
		break;
	default:
		cnt->others++;
		break;
	}
	SpinLockRelease(&InstrumentGlobal->lock);
}
#endif //PACKET_TRACE

/*
 * Since GPCC 4.9/6.1, we use metrics collector for batch sending packets.
 * Under non-extreme stress, most packets are send through metrics collector process
 * (It is stat sender process in GP5 and bgworker since GP6)
 * main_connected indicates the metrics_collector process has connected to ccagent,
 * this flag updates every MC_LOOP_SLEEP_MS (100ms), and read by each QD/QE.
 * If main_connected is false, each QD/QE can early return in query_info_collect_hook.
 * This ensures when GPCC is not started, metrics_collector extension do nothing,
 * to cut off performance impact and workaround potential bug.
 */
bool
should_keep_silence(void)
{
	return !(pbuff->main_connected);
}
