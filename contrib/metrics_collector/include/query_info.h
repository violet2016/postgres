/*-------------------------------------------------------------------------
 *
 * query_info.h
 *	  Definitions for query info struct and functions
 *
 * Copyright (c) 2017-Present Pivotal Software, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MC_QUERY_INFO_H
#define MC_QUERY_INFO_H

#include "common.h"
#define METRICS_PACKET_MAGIC            0x13c6e94b
#if GP_VERSION_NUM >= 60000
#define METRICS_PACKET_VERSION          (10) /* bump at GPCC 6.3 for gp6 send query tags */
#else
#define METRICS_PACKET_VERSION          (9) /* remain 9 for gp5 */
#endif
#define MAX_STRING_LEN                  (1024)
#define MAX_QUERY_PACKET_NUM            (100)
#define INVALID_PARENT_PLAN_NODE_ID     (-1)
#define DIR_GPCC_METRICS                ("gpmetrics")
#define DIR_QUERY_TEXT                  ("query_text")

// bits for query status
#define METRICS_QUERY_STATUS_SUBMIT     (512)
#define METRICS_QUERY_STATUS_START      (1024)
#define METRICS_QUERY_STATUS_DONE       (2048)
#define METRICS_QUERY_STATUS_ERROR      (4096)
#define METRICS_QUERY_STATUS_CANCELING  (8196)
#define METRICS_QUERY_STATUS_CANCELED   (16392)

/*
 * This defines the const for optimizer types
 * It should be exactly mapped with GPCC receiver side
 * Never change here alone
 * Refer: protos/query.proto EnumPlanGenerator
 */
#define GPCC_PLANGEN_INVALID            (0)
#define GPCC_PLANGEN_OPTIMIZER          (1)
#define GPCC_PLANGEN_PLANNER            (2)

typedef enum
{
	METRICS_PKTTYPE_NONE                     	= 0,
	METRICS_PKTTYPE_INIT_NODE                	= 20, /* Init node information from plan tree */
	METRICS_PKTTYPE_INSTR                    	= 21, /* Periodically send plan node progress info like tuplecount */
	METRICS_PKTTYPE_QUERY                    	= 22, /* Real time query submit/start/cancelling/cancelled event */
	METRICS_PKTTYPE_QUERY_TEXT               	= 23, /* Query string */
	METRICS_PKTTYPE_INIT_NODE_WITH_CONDITION 	= 24, /* Init node information from plan tree */
#if GP_VERSION_NUM >= 60000
	METRICS_PKTTYPE_QUERY_GUC                	= 25, /* Since GPCC 6.3 emit GUC info like gpcc.query_tags */
#endif
	METRICS_PKTTYPE_LOCK                     	= 30, /* Lock data like pg_locks */
	METRICS_PKTTYPE_ACTIVITY                 	= 40, /* Activity data like pg_stat_activity */
	METRICS_PKTTYPE_SPILLFILE                	= 50, /* Workfile usage info, gp_toolkit.gp_workfile_entries */
	METRICS_PKTTYPE_BATCH                    	= 90, /* A batch of packets can carry all kinds of messages */
	METRICS_PKTTYPE_START_NODE               	= 130, /* Start node information from plan tree */
	METRICS_PKTTYPE_DONE_NODE                	= 140, /* Done node information from plan tree */
	METRICS_PKTTYPE_ERROR_QUERY              	= 200, /* Real time query error event */
} metrics_packet_type;

/*
 * This enum defines the enum of CmdType
 * It should be exactly mapped with GPCC receiver side
 * Never change this enum alone
 */
typedef enum GPCCCmdType
{
	GPCC_CMD_UNKNOWN,
	GPCC_CMD_SELECT,
	GPCC_CMD_UPDATE,
	GPCC_CMD_INSERT,
	GPCC_CMD_DELETE,
	GPCC_CMD_UTILITY,
	GPCC_CMD_NOTHING
} GPCCCmdType;

/*
 * Translate GPDB CmdType to GPCC CmdType. This is a safe
 * guard to not break GPCC when GPDB changed CmdType enum
 */
#define SET_GPCC_CMDTYPE(pkt, operation) do {					\
	switch(operation)											\
	{															\
		case CMD_UNKNOWN:										\
			pkt.command_type = GPCC_CMD_UNKNOWN;				\
			break;												\
		case CMD_SELECT:										\
			pkt.command_type = GPCC_CMD_SELECT;					\
			break;												\
		case CMD_UPDATE:										\
			pkt.command_type = GPCC_CMD_UPDATE;					\
			break;												\
		case CMD_INSERT:										\
			pkt.command_type = GPCC_CMD_INSERT;					\
			break;												\
		case CMD_DELETE:										\
			pkt.command_type = GPCC_CMD_DELETE;					\
			break;												\
		case CMD_UTILITY:										\
			pkt.command_type = GPCC_CMD_UTILITY;				\
			break;												\
		case GPCC_CMD_NOTHING:									\
			pkt.command_type = GPCC_CMD_NOTHING;				\
			break;												\
	}															\
} while(0)

typedef struct metrics_query_id
{
	int32 tmid;            /* transaction time */
	int32 ssid;            /* session id */
	int32 ccnt;            /* command count */
} metrics_query_id;

typedef struct metrics_node_header
{
	int32             magic;
	int32             version;
	int32             pkttype;
	int32             dmy;
	double            timestamp;      /* timestamp of this event */
	int16             seg_id;
	int16             status;         /* plan node status */
	int32             slice_id;
	int32             proc_pid;
	metrics_query_id  qid;
} metrics_node_header;

typedef struct metrics_plan_init_node
{
	metrics_node_header     header;
	int32                   nid;                             /* node id */
	int32                   pnid;                            /* parent plan node id */
	int32                   node_type;                       /* node type */
	int32                   plan_width;                      /* plan_width from Plan */
	int32                   node_seq;                        /* node sequence for determine left/right tree */
	int32                   condition_length;                /* length of condition */
	double                  startup_cost;                    /* startup_cost from Plan */
	double                  total_cost;                      /* total_cost from Plan */
	double                  plan_rows;                       /* plan_rows from Plan */
	int16                   node_name_length;                /* length of node name */
	int16                   relation_name_length;            /* length of relation name */
	int16                   index_name_length;               /* length of index name */
	int16                   alias_name_length;               /* length of alias name */
	Oid                     relation_oid;                    /* OID of relation */
	int32                   dmy;
	char                    meta[NAMEDATALEN * 4];           /* node/relation/index/alias name */
} metrics_plan_init_node;

typedef struct metrics_plan_init_node_with_condition
{
	metrics_plan_init_node	init_node;						/* basic node info*/
	char                    condition[MAX_STRING_LEN];		/* condition */
} metrics_plan_init_node_with_condition;

typedef struct metrics_plan_start_node
{
	metrics_node_header header;
	int32               nid;                        /* node id */
	int32               dmy;
} metrics_plan_start_node;

typedef struct metrics_plan_done_node
{
	metrics_node_header header;
	int32               nid;                        /* node id */
	int32               dmy;                        /* skip 4 bytes */
	uint64              tuplecount;                 /* tuple count of a ndoe */
	uint64              ntuples;                    /* ntuple of a node */
	uint64              nloops;                     /* nloops of a node */
} metrics_plan_done_node;

typedef struct metrics_query_info
{
	int32            magic;
	int32            version;
	int32            pkttype;
	metrics_query_id qid;                           /* Query identity consist of tmid, ssid, ccnt */
	int16            user_length;                   /* Length of user in bytes */
	int16            db_length;                     /* Length of db in bytes */
	int16            priority_length;               /* Length of priority in bytes */
	int16            plan_gen;                      /* 0=invalid|1=orca|2=planner */
	double           tsubmit;                       /* timestamp of this event */
	double           tstart;                        /* timestamp of this event */
	double           tfinish;                       /* timestamp of this event */
	double           cost;                          /* plan tree total cost */
	int32            master_pid;                    /* Process pid of the backend process on master */
	int32            rsqid;                         /* Resource queue id */
	int32            rsgid;                         /* Resource group id */
	int16            status;                        /* Status of the query */
	int16            command_type;                  /* select|insert|update|delete */
	bool             enable_query_profiling;        /* whether bypass is turned on which will not omit the query
	                                                   whose runing time < 10s and database is gpperfmon */
	char             dmy[7];
	char             meta[NAMEDATALEN * 3];         /* Role name/DB name/priority */
} metrics_query_info;

typedef struct metrics_error_query_info
{
	int32            magic;
	int32            version;
	int32            pkttype;
	metrics_query_id qid;                           /* Query identity consist of tmid, ssid, ccnt */
	int32            master_pid;                    /* Process pid of the backend process on master */
	int32            error_length;                  /* error message length */
	double           timestamp;                     /* timestamp of this event */
	int16            status;                        /* Status of the query */
	char             dmy[6];
	char             error_message[MAX_STRING_LEN]; /* error message */
} metrics_error_query_info;

typedef struct metrics_query_text
{
	int32            magic;
	int32            version;
	int32            pkttype;
	int16            total;
	int16            seq_id;
	int32            length;
	metrics_query_id qid;
	char             content[MAX_STRING_LEN];
} metrics_query_text;

#if GP_VERSION_NUM >= 60000
/* GPCC 6.3 emit query tag */
typedef struct metrics_query_guc
{
	int32            magic;
	int32            version;
	int32            pkttype;
	int32            content_length;
	metrics_query_id qid;
	char             content[MAX_STRING_LEN];
} metrics_query_guc;
#endif

typedef struct plan_tree_walker_context
{
	Plan       *parent;
	QueryDesc  *qd;
	double      timestamp;
	int32       node_seq;
	List       *ancestors;
	Plan       *outer_plan;
	List       *deparse_cxt;
} plan_tree_walker_context;

/* Interface */
extern bool
should_skip_process(void);
extern void
emit_query_info(QueryDesc *qd, QueryMetricsStatus status);
extern void
EmitQueryInfo(QueryMetricsStatus, void *arg);
extern void
get_error_data(void *arg);
extern void
emit_plan_node_walker_multi(List *plans, PlanState **planstates, plan_tree_walker_context *context);
extern void
init_mc_error_data_hook(void);
extern size_t
plan_node_bytes_to_send(metrics_plan_init_node *pkt);
extern size_t
plan_node_with_condition_bytes_to_send(metrics_plan_init_node_with_condition *pkt);
extern size_t
query_info_bytes_to_send(metrics_query_info *pkt);
void
queryInfo_hook_finishup(void);

#endif   /* MC_QUERY_INFO_H */
