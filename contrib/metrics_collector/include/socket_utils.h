/*-------------------------------------------------------------------------
 *
 * socket_utils.h
 *	  Definitions for util functions
 *
 * Copyright (c) 2018-Present Pivotal Software, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MC_SOCKET_UTILS_H
#define MC_SOCKET_UTILS_H

#include "common.h"
#include "metrics_collector.h"
#include "query_info.h"

typedef struct metrics_conn
{
	int                sock;
	struct sockaddr_in addr;
} metrics_conn;

typedef struct batch_packet_header
{
	int32             magic;
	int32             version;
	int32             pkttype;
	int32             seg_id;
	int32             len;
} batch_packet_header;

#define PACKET_BUFFER_HEAD (sizeof(batch_packet_header) * 2) // x2 for safety

#if defined(__linux__)
/**
 * Before MPP-30545, the buffer size used to be 128K for best performance gains.
 * However, 128K is too large on some machines OS/network settings result in EMSGSIZE
 * error, so there was code to fallback to 64/32/16K on such error.
 * There was a bug that MC process and QE processes are not synced on the buffer
 * size. It causes memory overflow and crash the gpdb cluster.
 * It is of much complexity to keep the 128K and fall back to 16K for adaptability,
 * so in GPCC 6.3/4.11 we decided to reduce to a static 16K buffer.
 * The performance impact is tiny on low TPS. On extreme TPS (20K+), 16K buffer leads
 * less than 20% CPU impact to ccagent compared with 128K, it is acceptable because
 * the overall CPU consumption of ccagent is still very small, less than 3% on a
 * typical 40 core system.
 */
#define PACKET_BUFFER_SIZE (16 * 1024 - PACKET_BUFFER_HEAD)
#else
/**
 * On OSX data is send by UDP, the limit of packet size is https://en.wikipedia.org/wiki/User_Datagram_Protocol
 * Also needs to change the default UDP limit: sudo sysctl -w net.inet.udp.maxdgram=65535
 */
#define PACKET_BUFFER_SIZE (65507 - PACKET_BUFFER_HEAD)
#endif

#define SIZE_PACKET_MAGIC_AND_VERSION (8)
#define SINGLE_PACKET_SIZE_LIMIT      (1500)
#define PACKET_BUFFER_RED_LINE        (PACKET_BUFFER_SIZE - SINGLE_PACKET_SIZE_LIMIT)
#define NUM_PACKET_BUCKET             (32)
#define HASH_TO_BUCKET                (NUM_PACKET_BUCKET - 1)

#define MERGE_NONE       (0)
#define MERGE_QUERY_INFO (1)

/**
 * As unknown crash observed in MPP-30447, MPP-30545
 * We want to have extra protection when using FlushBuffMemContext
 * introduced since GPCC 4.9.0. This GUARD is to alloc some more
 * bytes to each buffer prepared for send_single_packet().
 */
#define FLUSH_BUFF_GUARD              (1024)

extern int packet_buffer_size_actual;
extern int packet_buffer_size_red_line_actual;

typedef struct PacketBufferHeader
{
	slock_t				lock;
	volatile int		current_pos;
	volatile int		snum;
	void			   *head;
	volatile void	   *current;
} PacketBufferHeader;

typedef struct PacketBufferBody
{
	char		buff[PACKET_BUFFER_SIZE];
} PacketBufferBody;

typedef struct PacketBuffer
{
	volatile bool      main_connected;
	PacketBufferHeader h[NUM_PACKET_BUCKET];
	PacketBufferBody   b[NUM_PACKET_BUCKET];
} PacketBuffer;

extern PacketBuffer        *pbuff;
extern MemoryContext        flushBuffContext;

extern void
flushBuffContext_finishup(void);

extern bool
should_init_socket(void);

extern int
socket_init(void);

extern int
send_single_packet(void *p, size_t n);

extern void
reset_conn(void);

extern void
send_packet(void *p, size_t n, int bucket, int send_packet);

extern void
merge_qlog_packet(metrics_query_info *pkt);

extern void
clear_merge_operations(PacketBufferHeader *pb_header);

extern bool
should_keep_silence(void);

#endif   /* MC_SOCKET_UTILS_H */
