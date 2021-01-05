/*-------------------------------------------------------------------------
 *
 * query_info.c
 *    Functions for sending query info packets
 *
 * Copyright (c) 2017-Present Pivotal Software, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "query_info.h"
#include "socket_utils.h"
#include "utils.h"
#include "utils_pg.h"
#include "tcop/utility.h"
#include "access/xact.h"

static int should_skip = 0; // 0: init; 1: don't skip; 2: do skip
static int session_id = 0;

// top_level_ccnt for handling gp_command_count grow in internal SPI/Function call
static int top_level_ccnt = 0;

// true for utility query who has no plan
// But for the utility query with plan like copy 'select *' or set distributed by,
// it will be reset in the same query, but there is no side-effect.
bool UtilityQuery = false;

// Store latest error data when error occurs
static ErrorData *errorData;

// memory context for queryInfo hook
static MemoryContext queryInfoCollectorContext = NULL;

// callback function invoked when error occurs
ErrorContextCallback *mc_error_callback = NULL;

// Original reset function of ErrorContext
void (*origin_error_reset_function) (MemoryContext context);

static void
emit_plan_tree(QueryDesc *qd);
static void
emit_plan_node_walker(PlanState *ps, plan_tree_walker_context *context);
static void
emit_plan_node(PlanState *ps,
               QueryMetricsStatus status);
static void
emit_plan_node_set(plan_tree_walker_context *context,
                   Plan *plan,
                   int32 parent_node_id,
                   QueryDesc *qd);
static void
emit_plan_node_with_condition(plan_tree_walker_context *context,
                              Plan *plan,
                              int32 parent_node_id,
                              QueryDesc *qd,
                              char* condition);
static void
init_plan_node_header(metrics_node_header *pkt_header,
                      double timestamp,
                      QueryMetricsStatus status,
                      metrics_packet_type pkt_type);
static void
set_initialized_plan_node_info(metrics_plan_init_node *pkt, Plan *plan, int32
parent_node_id, int32 node_seq, QueryDesc *qd);

static int16
query_string_packet_count(size_t total_len);
static void
emit_query_string(const QueryDesc *qd, QueryMetricsStatus status);
#if GP_VERSION_NUM >= 60000
extern void
emit_query_guc(const QueryDesc *qd, QueryMetricsStatus status);
#endif
void
emit_query_info(QueryDesc *qd, QueryMetricsStatus status);
static void
emit_error_query_info(QueryDesc *qd);
static void
mc_error_context_reset(MemoryContext context);

#ifdef MC_DEBUG_PRINT
static void
_print_debug_plannode(metrics_node_header *h, int32 nid, int32 pnid);
#endif // MC_DEBUG_PRINT

/*
 * the process should skip if:
 *  the GUC gp_enable_query_metrics is not turned on
 *  OR the GUC gpcc.enable_send_query_info is not turnned on
 *  OR agent is not startted
 *  OR the user is gpmon and the database is gpperfmon and gpcc.enable_query_profiling is not turned on
 */
bool
should_skip_process()
{
	const char *username;
	char       *dbname;

	if (!enable_query_metrics || !gpcc_enable_send_query_info)
		return true;

	// do nothing if agent not start
// FIXME: remove comment
//	if (should_keep_silence())
//		return true;

	if (session_id == 0)
	{
		session_id = session_id;
	}
	else if (session_id != session_id)
	{
		session_id = session_id;

		/*
		 * In some cases gpdb QD/QE process can be reset in CheckForResetSession()
		 * The process can change its session_id in this case.
		 * We are not very sure if gpdb has closed this UDS conn, here we can detect
		 * session id change and initiate and UDS conn reconnect to double ensure the
		 * connection to gpcc agent will do an accept again.
		 */
		reset_conn();
	}

	// The first time this QD/QE enters this function
	if (should_skip == 0)
	{
		// For each QD/QE, get into here only once
		should_skip = 1;

		// Username first because it is faster than DatabaseName
		// GPDB6_FIXME: in gpdb6 DefineCustomBoolVariable have GUC_GPDB_NEED_SYNC flags
		// so we don't have to filter gpcc internal queries in such ugly way
		// only IsTransactionState is true, we can run get_database_name safely
		username = get_username();
		if (username != NULL && strcmp("gpmon", username) == 0 && IsTransactionState())
		{
			dbname = get_database_name(MyDatabaseId); /* needs to be freed */
			if (dbname != NULL && strcmp("gpperfmon", dbname) == 0)
				should_skip = 2;
			if (dbname)
				pfree(dbname);
		}
	}
	// If we already know this process is gpmon/gpperfmon
	// and bypass is not turned on, skip immediately
	if (should_skip == 2 && gpcc_enable_query_profiling == false)
		return true;

	return false;
}

/*
 * Main hook function to emit query information.
 *
 * GPDB will call query_info_collect_hook on following events
 *
 *  - A query Submit/Start/Error/Cancelling/Cancelled/Done
 *    - args is QueryDesc
 *
 *  - A plan node Initialize/Start/Finish
 *    - for Initialize, args is QueryDesc which contains whole plan tree
 *    - for Start/Finish, args is the PlanState which only for data of that node
 */
void
EmitQueryInfo(QueryMetricsStatus status, void *args)
{
	/*
	 * If we are processing any utility query, internal query should be skipped
	 * It is because internal query bumps ccnt, leads to the finish packet of
	 * that top-level utility query have different ccnt than its submit/start
	 */
	if (UtilityQuery)
		return;

	if (should_skip_process())
		return;

#ifdef MC_DEBUG_PRINT
	// Log for debugging QLOG issue
	switch (status)
	{
		case METRICS_QUERY_SUBMIT:
		case METRICS_QUERY_START:
		case METRICS_QUERY_CANCELING:
		case METRICS_QUERY_CANCELED:
		{
			QueryDesc *qd = (QueryDesc*)args;
			// skip inner query
			if (qd && qd->plannedstmt && qd->plannedstmt->metricsQueryType > 0)
				break;
		}
		case METRICS_QUERY_DONE:
		case METRICS_QUERY_ERROR:
			elog(LOG, "QUERY_INFO(%d): %d-%d", status, session_id, gp_command_count);
			break;
		default:
			break;
	}
#endif

#ifndef NO_ERROR_RECOVERY
	PG_TRY();
	{
#endif
		switch (status)
		{
			case METRICS_PLAN_NODE_INITIALIZE:
				emit_plan_tree(args);
				break;

			case METRICS_PLAN_NODE_EXECUTING:
			case METRICS_PLAN_NODE_FINISHED:
			{
                PlanState *ps = args;
                emit_plan_node(ps, status);
				// FIXME: slice
                /*
				if (ps != NULL && ps->state != NULL)
				{
					int slice_id = LocallyExecutingSliceIndex(ps->state);
*/
					/*
					 * In two cases we should save plannode
					 * 1. A normal MPP query, which currentSliceId should equal to LocallyExecutingSliceIndex
					 * 2. Non-MPP only run on master, slice_id is always zero
					 */
                    /*
					if (slice_id == 0)
						emit_plan_node(ps, status);
				}*/
				break;
			}

			case METRICS_QUERY_SUBMIT:
				if(errorData)
				{
					FreeErrorData(errorData);
					errorData = NULL;
				}
			case METRICS_QUERY_START:
			case METRICS_QUERY_CANCELING:
			case METRICS_QUERY_CANCELED:
			{
				QueryDesc *qd = (QueryDesc*)args;
				// FIXME: skip inner query
			}
			case METRICS_QUERY_DONE:
			{
				emit_query_info(args, status);
				break;
			}
			case METRICS_QUERY_ERROR:
			{
				emit_error_query_info(args);
				break;
			}
			case METRICS_INNER_QUERY_DONE:
				// Do nothing
				break;
			default:
				ereport(WARNING, (errmsg("Metrics collector: invalid metrics status")));
		}
#ifndef NO_ERROR_RECOVERY
	}
	PG_CATCH();
	{
		// swallow any error in this hook
		FlushErrorState();
	}
	PG_END_TRY();
#endif
}

/*
 * callback function for ErrorContext
 * clean up last error data and copy newly-created error data
 */
void get_error_data(void *arg)
{
    // FIXME:
    /*
	if(elog_getelevel() < ERROR)
		return;
    */
	MemoryContext oldcontext = MemoryContextSwitchTo(queryInfoCollectorContext);
	if(errorData)
	{
		FreeErrorData(errorData);
		errorData = NULL;
	}
	errorData = CopyErrorData();
	MemoryContextSwitchTo(oldcontext);

	// The utility query will not call mppExecutorCleanup,
	// so send error info here
	// This branch will not run in GP5, for EmitUtilityQueryInfo will not be
	// called in GP5, so UtilityQuery is always false.
	if (UtilityQuery)
	{
		if (!should_skip_process())
		{
			QueryDesc  *qd = (QueryDesc *) palloc0(sizeof(QueryDesc));
			qd->plannedstmt = NULL;
			emit_error_query_info(qd);
			pfree(qd);
		}
		UtilityQuery = false;
	}
}

static void
emit_plan_node_walker(PlanState *ps, plan_tree_walker_context *context)
{
	QueryDesc  *qd;
	EState     *estate;
	Plan       *plan;
	int32      parent_node_id;
	bool       should_walk;
	bool       skip_outer = false;
	ListCell   *lst;
	StringInfo condition;

	if (ps == NULL)
		return;

	/* for plan node, emit plan node info with parent_node_id */
	plan   = (Plan *) ps->plan;

	qd     = context->qd;
	estate = qd->estate;

	context->node_seq++;
	should_walk = true;

	if (context->parent != NULL)
		parent_node_id = context->parent->plan_node_id;
	else
		parent_node_id = INVALID_PARENT_PLAN_NODE_ID;

	populate_outer_plan(context, plan);
	// FIXME:
    //condition = ExtractQual(ps, qd, context);
	if(condition && condition->len)
	{
		/* convert to UTF8 which is encoding for gpperfmon database */
		char *data = condition->data;
		if (GetDatabaseEncoding() != pg_get_client_encoding())
		{
			/**
			 * MPP-30606 QD crash when Client/Server encoding both set to WIN874
			 * In such case the text is converted at server side already, avoid convert it again.
			 * Only when client encoding and server encoding are different, do apply the conversion.
			 */
			data = (char *)pg_do_encoding_conversion((unsigned char*)condition->data,
			                                         condition->len, GetDatabaseEncoding(), PG_UTF8);
		}
		emit_plan_node_with_condition(context, plan, parent_node_id, qd, data);
	} else
	{
		emit_plan_node_set(context, plan, parent_node_id, qd);
	}

	if (should_walk)
	{
		/* Get ready to display the child plans */
		if (!planstate_has_children(ps))
			return;

		/* Continue walking */
		Plan *saved_parent = context->parent;
		Plan *saved_outer_plan = context->outer_plan;
		//Slice *saved_slice = context->slice;

		context->parent = plan;
		context->ancestors = lcons(ps, context->ancestors);

		/* initPlan-s */
		if (plan->initPlan)
		{
			foreach(lst, ps->initPlan)
			{
				SubPlanState *sps = (SubPlanState *) lfirst(lst);
				//SubPlan      *sp = (SubPlan *) sps->xprstate.expr;

				/* Subplan might have its own root slice */
                // FIXME:
                /*
				if (ps->state->es_sliceTable && sp->qDispSliceId > 0)
				{
					context->slice = (Slice *)list_nth(ps->state->es_sliceTable->slices,
					                                     sp->qDispSliceId);
				}*/
				emit_plan_node_walker(sps->planstate, context);
			}
		}

		/* lefttree */
		if (outerPlan(plan) && !skip_outer)
			emit_plan_node_walker(outerPlanState(ps), context);

		/* righttree */
		if (innerPlanState(ps))
			emit_plan_node_walker(innerPlanState(ps), context);

		/* special child plans */
		switch (nodeTag(plan))
		{
			case T_Append:
				emit_plan_node_walker_multi(((Append *) plan)->appendplans,
				                            ((AppendState *) ps)->appendplans,
				                            context);
				break;
			case T_BitmapAnd:
				emit_plan_node_walker_multi(((BitmapAnd *) plan)->bitmapplans,
				                            ((BitmapAndState *) ps)->bitmapplans,
				                            context);
				break;
			case T_BitmapOr:
				emit_plan_node_walker_multi(((BitmapOr *) plan)->bitmapplans,
				                            ((BitmapOrState *) ps)->bitmapplans,
				                            context);
				break;
			case T_SubqueryScan:
				emit_plan_node_walker(((SubqueryScanState *) ps)->subplan, context);
				break;
			default:
				walk_children(ps, plan, context);
				break;
		}

		/* subPlan-s */
		if (ps->subPlan)
		{
			foreach(lst, ps->subPlan)
			{
				SubPlanState *sps = (SubPlanState *) lfirst(lst);
				emit_plan_node_walker(sps->planstate, context);
			}
		}

		// Restore context
		context->ancestors = list_delete_first(context->ancestors);
		context->parent = saved_parent;
		context->outer_plan = saved_outer_plan;
		return;
	}
	return;
}

void
emit_plan_node_walker_multi(List *plans, PlanState **planstates, plan_tree_walker_context *context)
{
	int nplans = list_length(plans);
	int j;

	for (j = 0; j < nplans; j++)
		emit_plan_node_walker(planstates[j], context);
}

/*
 * Emit initialized plan tree
 * For each plan node, setup its parent node id, then send one packet.
 */
static void
emit_plan_tree(QueryDesc *qd)
{
	EState                   *estate;
	PlannedStmt              *plannedstmt;
	plan_tree_walker_context context;
	Plan                     *start_plan_node;
	instr_time               curr;

	plannedstmt        = qd->plannedstmt;
/*
	if (plannedstmt && plannedstmt->metricsQueryType > 0)
		return;
*/
	estate             = qd->estate;
	context.qd         = qd;
	context.parent     = NULL;
	context.ancestors  = NULL;
	context.outer_plan = NULL;
//	context.slice      = getCurrentSlice(estate, LocallyExecutingSliceIndex(estate));

	INSTR_TIME_SET_CURRENT(curr);
	context.timestamp = INSTR_TIME_GET_DOUBLE(curr);

	start_plan_node = plannedstmt->planTree;

	context.node_seq = start_plan_node->plan_node_id;

	init_deparse_cxt(&context, qd);

	/* Walk the root node of this slice */
	emit_plan_node_walker(qd->planstate, &context);
}

/*
 * Set initialized plan node information for packet.
 */
static void
set_initialized_plan_node_info(metrics_plan_init_node *pkt, Plan *plan, int32 parent_node_id, int32 node_seq, QueryDesc *qd)
{
	const char *pname;

	pkt->nid            = plan->plan_node_id;
	pkt->node_type      = plan->type;
	pkt->plan_width     = plan->plan_width;
	pkt->pnid           = parent_node_id;
	pkt->node_seq       = node_seq;
	pkt->startup_cost   = plan->startup_cost;
	pkt->total_cost     = plan->total_cost;
	pkt->plan_rows      = plan->plan_rows;
// FIXME:
//	pname                 = ExtractNodeName(plan, qd);
	pkt->node_name_length = strlen(pname);

	if (pkt->node_name_length > 0)
		memcpy(pkt->meta, pname, pkt->node_name_length);

	SetRelationInfo(plan, qd, pkt);
}

static void
emit_plan_node(PlanState *ps,
               QueryMetricsStatus status)
{
	instr_time curr;

	if (ps == NULL)
		return;
/*
	if (ps && ps->state && ps->state->es_plannedstmt &&
		ps->state->es_plannedstmt->metricsQueryType > 0)
		return;
*/
	INSTR_TIME_SET_CURRENT(curr);

	if (status == METRICS_PLAN_NODE_EXECUTING)
	{
		metrics_plan_start_node pkt;

		MemSet(&pkt, 0x00, sizeof(metrics_plan_start_node));
		init_plan_node_header(&(pkt.header),
		                      INSTR_TIME_GET_DOUBLE(curr),
		                      METRICS_PLAN_NODE_EXECUTING,
		                      METRICS_PKTTYPE_START_NODE);
		pkt.nid          = ps->plan->plan_node_id;
		if (pkt.header.qid.tmid == 0) {
			// Skip internal queries
			return;
		}
#ifdef MC_DEBUG_PRINT
		_print_debug_plannode(&(pkt.header), ps->plan->plan_node_id, -1);
#endif // MC_DEBUG_PRINT
		send_packet(&pkt, sizeof(pkt), pkt.header.qid.ssid & HASH_TO_BUCKET, MERGE_NONE);
	}
	else if (status == METRICS_PLAN_NODE_FINISHED)
	{
		metrics_plan_done_node pkt;

		MemSet(&pkt, 0x00, sizeof(metrics_plan_done_node));
		init_plan_node_header(&(pkt.header),
		                      INSTR_TIME_GET_DOUBLE(curr),
		                      METRICS_PLAN_NODE_FINISHED,
		                      METRICS_PKTTYPE_DONE_NODE);
		pkt.nid          = ps->plan->plan_node_id;

		// ps->instrument == NULL when run explain ...
		if (ps->instrument != NULL)
		{
			pkt.tuplecount   = ps->instrument->tuplecount;
			pkt.ntuples      = ps->instrument->ntuples;
			pkt.nloops       = ps->instrument->nloops;
		}
		if (pkt.header.qid.tmid == 0) {
			// Skip internal queries
			return;
		}
#ifdef MC_DEBUG_PRINT
		_print_debug_plannode(&(pkt.header), ps->plan->plan_node_id, -1);
#endif // MC_DEBUG_PRINT
		send_packet(&pkt, sizeof(pkt), pkt.header.qid.ssid & HASH_TO_BUCKET, MERGE_NONE);
	} else
	{
		elog(WARNING,"Metrics collector: unrecognized node status");
	}
}

size_t
plan_node_bytes_to_send(metrics_plan_init_node *pkt)
{
	const size_t offset = sizeof(metrics_plan_init_node) - NAMEDATALEN * 4;
	return offset + pkt->node_name_length + pkt->relation_name_length + pkt->index_name_length + pkt->alias_name_length;
}

static void
emit_plan_node_set(plan_tree_walker_context *context,
               Plan *plan,
               int32 parent_node_id,
               QueryDesc* qd)
{
	metrics_plan_init_node pkt;

	if (plan == NULL)
		return;

	MemSet(&pkt, 0x00, sizeof(metrics_plan_init_node));
	init_plan_node_header(&(pkt.header), context->timestamp,
	                      METRICS_PLAN_NODE_INITIALIZE, METRICS_PKTTYPE_INIT_NODE);

	if(pkt.header.qid.tmid == 0)
		return;

	set_initialized_plan_node_info(&pkt, plan, parent_node_id, context->node_seq, qd);

#ifdef MC_DEBUG_PRINT
	_print_debug_plannode(&(pkt.header), plan->plan_node_id, parent_node_id);
#endif // MC_DEBUG_PRINT
	send_packet(&pkt, plan_node_bytes_to_send(&pkt), pkt.header.qid.ssid & HASH_TO_BUCKET, MERGE_NONE);
}

size_t
plan_node_with_condition_bytes_to_send(metrics_plan_init_node_with_condition *pkt)
{
	const size_t offset = sizeof(metrics_plan_init_node_with_condition) - MAX_STRING_LEN - NAMEDATALEN * 4;
	int inner_size = pkt->init_node.node_name_length + pkt->init_node.relation_name_length + pkt->init_node.index_name_length + pkt->init_node.alias_name_length;
	inner_size += (8 - (inner_size % 8)) % 8;
	return offset + inner_size + pkt->init_node.condition_length;
}

static void
emit_plan_node_with_condition(plan_tree_walker_context *context,
                              Plan *plan,
                              int32 parent_node_id,
                              QueryDesc *qd,
                              char* condition)
{
	metrics_plan_init_node_with_condition pkt;
	int condition_len, valid_condition_len, inner_size;

	if (plan == NULL)
		return;

	MemSet(&pkt, 0x00, sizeof(metrics_plan_init_node_with_condition));
	init_plan_node_header(&(pkt.init_node.header),
	                      context->timestamp,
	                      METRICS_PLAN_NODE_INITIALIZE, METRICS_PKTTYPE_INIT_NODE_WITH_CONDITION);

	if(pkt.init_node.header.qid.tmid == 0)
		return;

	set_initialized_plan_node_info(&pkt.init_node, plan, parent_node_id, context->node_seq, qd);

	condition_len = strlen(condition);
	if (condition && condition_len)
	{
		valid_condition_len = condition_len;
		if (valid_condition_len > MAX_STRING_LEN)
		{
			valid_condition_len = cut_valid_str_by_len(PG_UTF8, condition, MAX_STRING_LEN);
		}

		inner_size = pkt.init_node.node_name_length + pkt.init_node.relation_name_length + pkt.init_node.index_name_length + pkt.init_node.alias_name_length;
		inner_size += (8 - (inner_size % 8)) % 8;
		memcpy(pkt.condition - NAMEDATALEN * 4 + inner_size, condition, valid_condition_len);

		pkt.init_node.condition_length = valid_condition_len;
	}

#ifdef MC_DEBUG_PRINT
	_print_debug_plannode(&(pkt.init_node.header), plan->plan_node_id, parent_node_id);
#endif // MC_DEBUG_PRINT
	send_packet(&pkt, plan_node_with_condition_bytes_to_send(&pkt), pkt.init_node.header.qid.ssid & HASH_TO_BUCKET, MERGE_NONE);
}

static void
init_plan_node_header(metrics_node_header *pkt_header,
                      double timestamp,
                      QueryMetricsStatus status,
                      metrics_packet_type pkt_type)
{
	memset(pkt_header, 0x00, sizeof(metrics_node_header));

	pkt_header->magic     = METRICS_PACKET_MAGIC;
	pkt_header->version   = METRICS_PACKET_VERSION;
	pkt_header->pkttype   = pkt_type;
	pkt_header->timestamp = timestamp;
	pkt_header->qid.tmid  = mc_gettmid();
	pkt_header->qid.ssid  = session_id;
	pkt_header->qid.ccnt  = 0; //FIXME: gp_command_count;
	pkt_header->seg_id    = 0;
	pkt_header->proc_pid  = MyProcPid;
	pkt_header->status    = status;
	//pkt_header->slice_id  = currentSliceId;
}

size_t
query_info_bytes_to_send(metrics_query_info *pkt)
{
	const size_t offset = sizeof(metrics_query_info) - NAMEDATALEN * 3;
	return offset + pkt->user_length + pkt->db_length + pkt->priority_length;
}

/*
 * Emit query info:
 *  - query basic info: username, database, pid etc.
 *  - query status: submit, start, error, done etc.
 *  - query text, maximum length is 25K
 */
void
emit_query_info(QueryDesc *qd, QueryMetricsStatus status)
{
	metrics_query_info  pkt;
	instr_time          curr;
	char               *priority;

	if (qd == NULL)
		return;

	memset(&pkt, 0x00, sizeof(metrics_query_info));

	pkt.magic    = METRICS_PACKET_MAGIC;
	pkt.version  = METRICS_PACKET_VERSION;
	pkt.pkttype  = METRICS_PKTTYPE_QUERY;
	pkt.qid.tmid = mc_gettmid();
	//FIXME: now all tmid is 0
	/*
	if (pkt.qid.tmid == 0) {
		//Skip internal queries
		return;
	}*/

	pkt.enable_query_profiling = gpcc_enable_query_profiling;
	pkt.qid.ssid = session_id;
    // FIXME: command count
	/*
    if (status == METRICS_QUERY_SUBMIT)
		top_level_ccnt = gp_command_count;*/
	pkt.qid.ccnt = top_level_ccnt;

	INSTR_TIME_SET_CURRENT(curr);
	switch (status)
	{
	case METRICS_QUERY_SUBMIT:
		pkt.tsubmit = INSTR_TIME_GET_DOUBLE(curr);
		pkt.status  = METRICS_QUERY_STATUS_SUBMIT;
		break;
	case METRICS_QUERY_START:
		pkt.tstart = INSTR_TIME_GET_DOUBLE(curr);
		pkt.status  = METRICS_QUERY_STATUS_START;
		break;
	case METRICS_QUERY_DONE:
		pkt.tfinish = INSTR_TIME_GET_DOUBLE(curr);
		pkt.status  = METRICS_QUERY_STATUS_DONE;
		break;
	case METRICS_QUERY_CANCELED:
		pkt.tfinish = INSTR_TIME_GET_DOUBLE(curr);
		pkt.status  = METRICS_QUERY_STATUS_CANCELED;
		break;
	case METRICS_QUERY_CANCELING:
		pkt.status  = METRICS_QUERY_STATUS_CANCELING;
		break;
	default:
		break;
	}
	pkt.master_pid = MyProcPid;

	if (status < METRICS_QUERY_START && qd->plannedstmt != NULL)
	{
        pkt.plan_gen = GPCC_PLANGEN_PLANNER;
		if (qd->plannedstmt->planTree != NULL)
		{
			pkt.cost = qd->plannedstmt->planTree->total_cost;
		}
	}

	/*
	 * Only emit following info once, with submit event
	 *  - username
	 *  - database name
	 *  - resource queue id and priority
	 *  - resource group id
	 *  - command type
	 */
	if (status == METRICS_QUERY_SUBMIT)
	{
// FIXME:

	//	const char *username;
	//	char       *dbname;

	//	username = get_username();  /* no need to free freed */
	//	if (username)
	//	{
	//		pkt.user_length = strlen(username);
	//		memcpy(pkt.meta, username, pkt.user_length);
	//	}

	//	dbname = get_database_name(MyDatabaseId); /* needs to be freed */
	//	if (dbname)
	//	{
	//		pkt.db_length = strlen(dbname);
	//		memcpy(pkt.meta+pkt.user_length, dbname, pkt.db_length);
	//		pfree(dbname);
	//	}

		SET_GPCC_CMDTYPE(pkt, qd->operation);
		/*
        pkt.rsqid        = GetResQueueId();
		priority         = GetResqueuePriority(pkt.rsqid);
		if (priority)
		{
			pkt.priority_length = strlen(priority);
			memcpy(pkt.meta+pkt.user_length+pkt.db_length, priority, pkt.priority_length);
			pfree(priority);
		}*/
	}

	//if (status == METRICS_QUERY_SUBMIT)
	send_packet(&pkt, query_info_bytes_to_send(&pkt), pkt.qid.ssid & HASH_TO_BUCKET, MERGE_NONE);
	//else
	//	merge_qlog_packet(&pkt);

	if (status == METRICS_QUERY_SUBMIT)
	{
#if GP_VERSION_NUM >= 60000
		emit_query_guc(qd, status);
#endif
		emit_query_string(qd, status);
	}
}

/*
 * Emit query info for error query
 */
static void
emit_error_query_info(QueryDesc *qd)
{
	metrics_error_query_info pkt;
	instr_time               curr;
	int32                    actual_error_length = 0;

// FIXME:
	if (qd == NULL)
		return;


	memset(&pkt, 0x00, sizeof(metrics_error_query_info));

	pkt.magic    = METRICS_PACKET_MAGIC;
	pkt.version  = METRICS_PACKET_VERSION;
	pkt.pkttype  = METRICS_PKTTYPE_ERROR_QUERY;
	pkt.qid.tmid = mc_gettmid();
	if (pkt.qid.tmid == 0) {
		//Skip internal queries
		return;
	}

	pkt.qid.ssid = session_id;
	pkt.qid.ccnt = top_level_ccnt;

	INSTR_TIME_SET_CURRENT(curr);
	pkt.timestamp = INSTR_TIME_GET_DOUBLE(curr);

	if (errorData) {
		actual_error_length = strlen(errorData->message);
	}
	if (actual_error_length > 0)
	{
		if (actual_error_length <= MAX_STRING_LEN)
			pkt.error_length = actual_error_length;
		else
			pkt.error_length = cut_valid_str_by_len(PG_UTF8, errorData->message, MAX_STRING_LEN);

		memcpy(pkt.error_message, errorData->message, pkt.error_length);
	}

	pkt.master_pid   = MyProcPid;
	pkt.status       = METRICS_QUERY_STATUS_ERROR;

	send_packet(&pkt, sizeof(metrics_error_query_info) - MAX_STRING_LEN + pkt.error_length, pkt.qid.ssid & HASH_TO_BUCKET, MERGE_NONE);
}

/*
 * Emit query text, do this when status is METRICS_QUERY_SUBMIT
 * If query is huge, then only emit part of the whole query string.
 */
static void
emit_query_string(const QueryDesc *qd, QueryMetricsStatus status)
{
	metrics_query_text pkt;
	size_t             query_text_len;
	int16              total_packets;
	int16              packets_tosend;
	int16              i;
	char*              query_text;

	memset(&pkt, 0x00, sizeof(metrics_query_text));

	pkt.magic    = METRICS_PACKET_MAGIC;
	pkt.version  = METRICS_PACKET_VERSION;
	pkt.pkttype  = METRICS_PKTTYPE_QUERY_TEXT;
	pkt.qid.tmid = mc_gettmid();
	if (pkt.qid.tmid == 0) {
		//Skip internal queries
		return;
	}

	pkt.qid.ssid = session_id;
	pkt.qid.ccnt = 0;

	/* convert to UTF8 which is encoding for gpperfmon database */
	query_text = (char *)qd->sourceText;
	if (GetDatabaseEncoding() != pg_get_client_encoding())
	{
		/**
		 * MPP-30606 QD crash when Client/Server encoding both set to WIN874
		 * In such case the text is converted at server side already, avoid convert it again.
		 * Only when client encoding and server encoding are different, do apply the conversion.
		 */
		query_text = (char *)pg_do_encoding_conversion((unsigned char*)qd->sourceText,
		                                               strlen(qd->sourceText), GetDatabaseEncoding(), PG_UTF8);
	}
	query_text_len = strlen(query_text);
	total_packets  = query_string_packet_count(query_text_len);

	if (query_text_len > MAX_QUERY_PACKET_NUM * MAX_STRING_LEN)
	{
		FILE * queryTextFile;
		char queryTextFilePath[MAXPGPATH],
				queryTextFileName[MAXPGPATH];
		query_text_len = cut_valid_str_by_len(PG_UTF8, query_text,
		                                      MAX_QUERY_PACKET_NUM * MAX_STRING_LEN);

		// Write query text to file
		snprintf(queryTextFileName, NAMEDATALEN, "q%d-%d-%d.txt", pkt.qid
				.tmid, pkt.qid.ssid, pkt.qid.ccnt);
		join_path_components(queryTextFilePath, DIR_GPCC_METRICS, DIR_QUERY_TEXT);
		join_path_components(queryTextFilePath, queryTextFilePath, queryTextFileName);
		canonicalize_path(queryTextFilePath);
		queryTextFile = fopen(queryTextFilePath, "w");
		if (queryTextFile == NULL)
		{
			elog(LOG, "Metrics collector: error when open query text file %s", queryTextFilePath);
		}
		else
		{
			if (fprintf(queryTextFile, "%s\n", query_text) < 0)
				elog(LOG, "Metrics collector: error when write query text to %s", queryTextFilePath);

			fclose(queryTextFile);
		}
	}

	packets_tosend =
		total_packets > MAX_QUERY_PACKET_NUM ? (int16) MAX_QUERY_PACKET_NUM
		                                  : total_packets;

	pkt.total = total_packets;

	for (i = 0; i < packets_tosend; i++)
	{
		size_t copy_length, leftover;

		pkt.seq_id = i;

		leftover    = query_text_len - i * MAX_STRING_LEN;
		copy_length = (leftover < MAX_STRING_LEN) ? leftover : MAX_STRING_LEN;

		MemSet(pkt.content, 0x00, MAX_STRING_LEN);
		memcpy(pkt.content,
		       query_text + i * MAX_STRING_LEN,
		       copy_length);

		pkt.length = copy_length;

		send_packet(&pkt, sizeof(metrics_query_text) - MAX_STRING_LEN + copy_length, pkt.qid.ssid & HASH_TO_BUCKET, MERGE_NONE);
	}
}

static int16
query_string_packet_count(size_t total_len)
{
	if (total_len <= MAX_STRING_LEN)
		return 1;

	return total_len % MAX_STRING_LEN == 0
	       ? (int16) (total_len / MAX_STRING_LEN)
	       : (int16) (total_len / MAX_STRING_LEN + 1);
}

/*
 * Function to initialize mc error hooks and memory context
 */
void
init_mc_error_data_hook(void)
{
	MemoryContext oldcontext;
	// add callback function in error context
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	mc_error_callback = (ErrorContextCallback *) palloc0(sizeof(ErrorContextCallback));

	/* Set up a callback for error reporting */
	mc_error_callback->callback = get_error_data;
	mc_error_callback->previous = error_context_stack;
	error_context_stack = mc_error_callback;

	MemoryContextSwitchTo(oldcontext);
// FIXME:
/*
	if(ErrorContext != NULL)
	{
		origin_error_reset_function = ErrorContext->methods.reset;
		ErrorContext->methods.reset = mc_error_context_reset;
	}
*/
	// Only for errData now
	queryInfoCollectorContext = AllocSetContextCreate(TopMemoryContext,
	                                                  "QueryInfoCollectorMemCtxt",
	                                                  ALLOCSET_DEFAULT_INITSIZE,
	                                                  ALLOCSET_DEFAULT_INITSIZE,
	                                                  ALLOCSET_DEFAULT_MAXSIZE);
}

/*
 * When an query error occurs, erreport will invoke this function
 * so that we can copy errorData to our memory context then emit
 * the error message.
 */
static void
attach_error_callback(void)
{
	bool already_attached = false;
	ErrorContextCallback *econtext;
	for (econtext = error_context_stack; econtext != NULL; econtext = econtext->previous)
	{
		if(econtext->callback == get_error_data)
		{
			already_attached = true;
		}
	}
	if(already_attached == false)
	{
		MemoryContext oldcontext;
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		if(mc_error_callback == NULL)
		{
			ereport(LOG, (errmsg("Metrics collector: mc_error_callback is empty")));
		}
		else
		{
			mc_error_callback->previous = error_context_stack;
			error_context_stack = mc_error_callback;
		}
		MemoryContextSwitchTo(oldcontext);
	}
}

/*
 * when an query error occurs, error_context_stack will be cleaned up
 * and the ErrorContext will be reset, too. So we put this in the reset
 * function of ErrorContext to reattach the mc_error_callback function.
 */
static void
mc_error_context_reset(MemoryContext context)
{
	if(ErrorContext == NULL || context != ErrorContext)
	{
		ereport(LOG, (errmsg("Metrics collector: metrics collector only can reset error context")));
		return;
	}
	if(origin_error_reset_function == NULL)
	{
		ereport(LOG, (errmsg("Metrics collector: origin_error_reset_function is empty")));
		return;
	}
	attach_error_callback();
	origin_error_reset_function(context);
}

void
queryInfo_hook_finishup(void)
{
	if (queryInfoCollectorContext)
	{
		MemoryContextDelete(queryInfoCollectorContext);
		queryInfoCollectorContext = NULL;
	}
}

#ifdef MC_DEBUG_PRINT
static void
_print_debug_plannode(metrics_node_header *h, int32 nid, int32 pnid)
{
	elog(LOG,
		"EMIT PLAN NODE (%d %d) (%d) %d %d (%d) (%d-%d-%d)",
		nid,
		pnid,
		h->slice_id,
		h->seg_id,
		h->proc_pid,
		h->status,
		h->qid.tmid,
		h->qid.ssid,
		h->qid.ccnt);
}
#endif // MC_DEBUG_PRINT
