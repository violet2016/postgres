/*-------------------------------------------------------------------------
 *
 * utils.c
 * Utility function for metrics collector
 *
 * Copyright (c) 2017-Present Pivotal Software, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "utils.h"
#include "utils_pg.h"

void
AppendQualStringInfo(StringInfo condition, StringInfo append)
{
	if(!append)
		return;

	if(append->len <= 0)
		return;

	if(condition->len > 0)
		appendStringInfoChar(condition, '\n');

	appendStringInfoString(condition, append->data);
}

StringInfo
extract_scan_qual(List *qual, const char *qlabel, PlanState *planstate, plan_tree_walker_context *context)
{
	List *ctx;
	bool useprefix;
	Node *node;
	char *exprstr;

	/* No work if empty qual */
	if (qual == NIL)
        return NULL;

	StringInfo res = makeStringInfo();
	useprefix      = (IsA(planstate->plan, SubqueryScan));

	/* Convert AND list to explicit AND */
	node = (Node *) make_ands_explicit(qual);

	/* Set up deparsing context */
	ctx = deparse_context_for_qual(planstate, context);

	/* Deparse the expression */
	exprstr = deparse_expr_for_qual(node, ctx, useprefix, false);

	/* And add to str */
	appendStringInfo(res, "%s: %s", qlabel, exprstr);
	return res;
}

StringInfo
extract_sort_keys(SortState *sortstate, plan_tree_walker_context *context)
{
	Sort		*sort = (Sort *) sortstate->ss.ps.plan;
	Plan		*plan = ((PlanState *)sortstate)->plan;
	List		*ctx;
	bool		useprefix;
	int			keyno;
	char		*exprstr;
	bool		first_flag = true;
	int			nkeys = sort->numCols;

	if (nkeys <= 0)
		return NULL;

	QueryDesc   *qd          = context->qd;
	AttrNumber  *keycols     = sort->sortColIdx;
	StringInfo   res         = makeStringInfo();
	PlannedStmt *plannedStmt = qd->plannedstmt;

	appendStringInfoString(res, "Sort Key: ");

	useprefix = list_length(plannedStmt->rtable) > 1;

	/* Set up deparsing context */
	Plan *saved_outer_plan = context->outer_plan;
	context->outer_plan = NULL;
	ctx = deparse_context_for_qual((PlanState *)sortstate, context);
	context->outer_plan = saved_outer_plan;

	for (keyno = 0; keyno < nkeys; keyno++)
	{
		/* find key expression in tlist */
		AttrNumber	keyresno = keycols[keyno];
		TargetEntry *target = get_tle_by_resno(plan->targetlist,
                                               keyresno);

		if (target)
		{
			/* Deparse the expression, showing any top-level cast */
			exprstr = deparse_expr_for_qual((Node *) target->expr, ctx, useprefix, true);
			/* And add to str */
			if (first_flag)
			{
				appendStringInfo(res, "%s", exprstr);
				first_flag = false;
			}
			else
			{
				appendStringInfo(res, ", %s", exprstr);
			}
		}
	}
	return res;
}

StringInfo
extract_upper_qual(List *qual, PlanState *planstate, plan_tree_walker_context *context, const char *qlabel)
{
	List		*ctx;
	bool		useprefix;
	Node		*node;
	char		*exprstr;

	/* No work if empty qual */
	if (qual == NIL)
		return NULL;

	QueryDesc  *qd  = context->qd;
	StringInfo  res = makeStringInfo();
	useprefix       = list_length(qd->plannedstmt->rtable) > 1;

	/* Deparse the expression */
	node = (Node *) make_ands_explicit(qual);

	/* Set up deparsing context */
	Plan *saved_outer_plan = context->outer_plan;
	context->outer_plan = NULL;
	ctx = deparse_context_for_qual(planstate, context);
	context->outer_plan = saved_outer_plan;

	exprstr = deparse_expr_for_qual(node, ctx, useprefix, false);

	/* And add to str */
	appendStringInfo(res, "%s: %s", qlabel, exprstr);
	return res;
}

/*
 * This function valid mbstr by given encoding, return a byte len <= input len
 */
int
cut_valid_str_by_len(int encoding, const char *mbstr, int len)
{
	mbverifier	mbverify;
	int			mb_len;

	Assert(PG_VALID_ENCODING(encoding));

	/*
	 * In single-byte encodings, we need only reject nulls (\0).
	 */
	if (pg_encoding_max_length(encoding) <= 1)
	{
		const char *nullpos = memchr(mbstr, 0, len);

		if (nullpos == NULL)
			return len;
		return nullpos - mbstr;
	}

	/* fetch function pointer just once */
	mbverify = pg_wchar_table[encoding].mbverify;

	mb_len = 0;

	while (len > 0)
	{
		int l;

		/* fast path for ASCII-subset characters */
		if (!IS_HIGHBIT_SET(*mbstr))
		{
			if (*mbstr != '\0')
			{
				mb_len++;
				mbstr++;
				len--;
				continue;
			}
			return mb_len;
		}

		l = (*mbverify) ((const unsigned char *) mbstr, len);

		if (l < 0)
			return mb_len;

		mbstr += l;
		len -= l;
		mb_len += l;
	}
	return mb_len;
}

int32
mc_gettmid(void)
{
	//FIXME:
	return 0;
}

void
init_guc(void)
{
	DefineCustomBoolVariable("gpcc.enable_send_query_info",
				 gettext_noop("Enable query info metrics collection."),
				 NULL,
				 &gpcc_enable_send_query_info,
				 false,
				 PGC_USERSET,
				 0,
				 NULL, NULL, NULL);

	DefineCustomBoolVariable("gpcc.enable_query_profiling",
				 gettext_noop("Enable query profiling."),
				 NULL,
				 &gpcc_enable_query_profiling,
				 false,
				 PGC_USERSET,
				 0,
				 NULL, NULL, NULL);
	DefineCustomIntVariable("gpcc.query_metrics_port",
				 gettext_noop("Sets the port number of sending query metrics."),
				 NULL,
				 &gpcc_query_metrics_port,
				 9898,
				 1024, 65535,
				 PGC_USERSET,
				 0,
				 NULL, NULL, NULL);

	DefineCustomIntVariable("gpcc.metrics_collector_pkt_version",
				 gettext_noop("Metrics Collector packet version."),
				 NULL,
				 &metrics_collector_pkt_version,
				 METRICS_PACKET_VERSION,
				 0, 65535,
				 PGC_INTERNAL,
				 0,
				 NULL, NULL, NULL);
}

metrics_lock_t*
populate_lock_data(LockData *lockData, int *i, int tmid)
{
	metrics_lock_t   *lr = NULL;
	bool              granted = false;
	LOCKMODE          mode = 0;
	PROCLOCK         *proclock;
	LOCK             *lock;
	PGPROC           *proc;

	//FIXME: change to BlockedProcData
	/*
	//proclock = &(lockData->proclocks[*i]);
	lock     = &(lockData->locks[*i]);
	//proc     = &(lockData->procs[*i]);
	if (proclock->holdMask)
	{
		for (mode = 0; mode < MAX_LOCKMODES; mode++)
		{
			if (proclock->holdMask & LOCKBIT_ON(mode))
			{
				granted = true;
				proclock->holdMask &= LOCKBIT_OFF(mode);
				break;
			}
		}
	}
	if (!granted)
	{
		if (proc->waitLock == proclock->tag.myLock)
		{
			mode = proc->waitLockMode;
			(*i)++;
		}
		else
		{
			(*i)++;
			return NULL;
		}
	}

	lr = (metrics_lock_t *) palloc0(sizeof(metrics_lock_t));
	lr->granted             = granted;
	lr->mode                = mode;
	lr->tbackendid          = proc->backendId;
	lr->tlocaltransactionid = proc->lxid;
	if (proc->pid != 0)
		lr->pid             = proc->pid;
	lr->tmid                = tmid;
	// FIXME:
	//lr->queryCommandId      = proc->queryCommandId;

	populate_lock_tag(lr, &(lock->tag));
	*/
	return lr;
}

// ExtractNodeName will extract node type name from a plan node
const char*
ExtractNodeName(Plan *plan, QueryDesc *qd)
{
	const char *pname = NULL;

	switch (nodeTag(plan))
	{
		case T_Result:
			pname = "Result";
			break;
		case T_Append:
			pname = "Append";
			break;
		case T_RecursiveUnion:
			pname = "Recursive Union";
			break;
		case T_BitmapAnd:
			pname = "BitmapAnd";
			break;
		case T_BitmapOr:
			pname = "BitmapOr";
			break;
		case T_NestLoop:
			switch (((NestLoop *) plan)->join.jointype)
			{
				case JOIN_INNER:
					pname = "Nested Loop";
					break;
				case JOIN_LEFT:
					pname = "Nested Loop Left Join";
					break;
				case JOIN_FULL:
					pname = "Nested Loop Full Join";
					break;
				case JOIN_RIGHT:
					pname = "Nested Loop Right Join";
					break;
				default:
					pname = "Nested Loop Join";
					break;
			}
			break;
		case T_MergeJoin:
			switch (((MergeJoin *) plan)->join.jointype)
			{
				case JOIN_INNER:
					pname = "Merge Join";
					break;
				case JOIN_LEFT:
					pname = "Merge Left Join";
					break;
				case JOIN_FULL:
					pname = "Merge Full Join";
					break;
				case JOIN_RIGHT:
					pname = "Merge Right Join";
					break;
				default:
					pname = "Merge Join";
					break;
			}
			break;
		case T_HashJoin:
			switch (((HashJoin *) plan)->join.jointype)
			{
				case JOIN_INNER:
					pname = "Hash Join";
					break;
				case JOIN_LEFT:
					pname = "Hash Left Join";
					break;
				case JOIN_FULL:
					pname = "Hash Full Join";
					break;
				case JOIN_RIGHT:
					pname = "Hash Right Join";
					break;
					break;
				default:
					pname = "Hash Join";
					break;
			}
			break;
		case T_SeqScan:
			pname = "Seq Scan";
			break;
		case T_IndexScan:
			pname = "Index Scan";
			break;
		case T_BitmapIndexScan:
			pname = "Bitmap Index Scan";
			break;
		case T_BitmapHeapScan:
			pname = "Bitmap Heap Scan";
			break;
		case T_TidScan:
			pname = "Tid Scan";
			break;
		case T_SubqueryScan:
			pname = "Subquery Scan";
			break;
		case T_FunctionScan:
			pname = "Function Scan";
			break;
		case T_ValuesScan:
			pname = "Values Scan";
			break;
		case T_CteScan:
			pname = "CTE Scan";
			break;
		case T_WorkTableScan:
			pname = "WorkTable Scan";
			break;
		case T_Material:
			pname = "Materialize";
			break;
		case T_Sort:
			pname = "Sort";
			break;
		case T_Agg:
			switch (((Agg *) plan)->aggstrategy)
			{
				case AGG_PLAIN:
					pname = "Aggregate";
					break;
				case AGG_SORTED:
					pname = "GroupAggregate";
					break;
				case AGG_HASHED:
					pname = "HashAggregate";
					break;
				default:
					pname = "Aggregate";
					break;
			}
			break;
		case T_Unique:
			pname = "Unique";
			break;
		case T_SetOp:
			switch (((SetOp *) plan)->cmd)
			{
				case SETOPCMD_INTERSECT:
					pname = "SetOp Intersect";
					break;
				case SETOPCMD_INTERSECT_ALL:
					pname = "SetOp Intersect All";
					break;
				case SETOPCMD_EXCEPT:
					pname = "SetOp Except";
					break;
				case SETOPCMD_EXCEPT_ALL:
					pname = "SetOp Except All";
					break;
				default:
					pname = "SetOp";
					break;
			}
			break;
		case T_Limit:
			pname = "Limit";
			break;
		case T_Hash:
			pname = "Hash";
			break;
		default:
			pname = "Unknown";
			break;
	}
	return pname;
}

/*
 * SetRelationInfo populate following fields into pkt_node
 *  - index_name
 *  - relation_name
 *  - alias_name
 *  - relation_oid
 */
void
SetRelationInfo(Plan *plan, QueryDesc *qd, metrics_plan_init_node *pkt_node)
{
	Index              rti;
	RangeTblEntry     *rte = NULL;
	char              *objectname = NULL;
	char              *refname = NULL;
	const char        *indexname = NULL;

	if(qd == NULL)
		return;

	StringInfo name = makeStringInfo();

	switch nodeTag(plan) {
		case T_SeqScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_WorkTableScan:
		case T_IndexScan:
		case T_FunctionScan:
			if (((Scan *) plan)->scanrelid > 0)
			{
				rti = ((Scan *) plan)->scanrelid;
				rte = rt_fetch(rti, qd->plannedstmt->rtable);
			}
			break;
		default:
			break;
	}

	if (rte != NULL)
	{
		if (rte->alias)
		{
			/* If RTE has a user-defined alias, prefer that */
			refname = rte->alias->aliasname;
		}
		else
		{
			/* Otherwise use whatever the parser assigned */
			refname = rte->eref->aliasname;
		}
	}

	switch nodeTag(plan)
	{
		case T_IndexScan:
			/* Set index name */
			if (explain_get_index_name_hook)
				indexname = (*explain_get_index_name_hook) (((IndexScan *) plan)->indexid);
			else
				indexname = NULL;
			if (indexname == NULL)
			{
				indexname = get_rel_name(((IndexScan *) plan)->indexid);
				// No need to quote_identifier
			}
			/* FALL THRU */
		case T_SeqScan:
		case T_BitmapHeapScan:
		case T_TidScan:
			if (rte != NULL)
			{
				/* Assume it's on a real relation */
				Assert(rte->rtekind == RTE_RELATION);
				pkt_node->relation_oid = rte->relid;
				objectname = get_rel_name(rte->relid);
			}
			break;

		case T_BitmapIndexScan:
			// Set index node`s index name
			if (explain_get_index_name_hook)
				indexname = (*explain_get_index_name_hook) (((BitmapIndexScan *) plan)->indexid);
			else
				indexname = NULL;
			if (indexname == NULL)
			{
				indexname = get_rel_name(((BitmapIndexScan *) plan)->indexid);
				// No need to quote_identifier
			}
			break;

		case T_SubqueryScan:
		case T_ValuesScan:
			break;

		case T_FunctionScan:
			if (rte != NULL)
			{
				Node	   *funcexpr;

				/*
				 * If the expression is still a function call, we can get the
				 * real name of the function.  Otherwise, punt (this can
				 * happen if the optimizer simplified away the function call,
				 * for example).
				 */
				// FIXME: func expr
				/*
				funcexpr = ((FunctionScan *) plan)->funcexpr;
				if (funcexpr && IsA(funcexpr, FuncExpr))
				{
					Oid			funcid = ((FuncExpr *) funcexpr)->funcid;
				*/
					/* We only show the func name, not schema name */
				/*
					objectname = get_func_name(funcid);
					refname = rte->eref->aliasname;
				}
				else*/
				objectname = rte->eref->aliasname;
			}
			break;

		case T_CteScan:
		case T_WorkTableScan:
			if (rte != NULL)
			{
				objectname = rte->ctename;
			}
			break;

		default:
			break;
	}

	if (objectname != NULL)
	{
		appendStringInfoString(name, objectname); // No need to quote_identifier
		pkt_node->relation_name_length = strlen(name->data);
		if (pkt_node->relation_name_length > 0)
			memcpy(pkt_node->meta + pkt_node->node_name_length, name->data, pkt_node->relation_name_length);
		resetStringInfo(name);
	}
	if (indexname != NULL)
	{
		appendStringInfoString(name, indexname); // No need to quote_identifier
		pkt_node->index_name_length = strlen(name->data);
		if (pkt_node->index_name_length > 0)
			memcpy(pkt_node->meta + pkt_node->node_name_length + pkt_node->relation_name_length, name->data, pkt_node->index_name_length);
		resetStringInfo(name);
	}
	if (refname != NULL)
	{
		if (objectname == NULL || strcmp(refname, objectname) != 0)
		{
			appendStringInfoString(name, refname); // No need to quote_identifier
			pkt_node->alias_name_length = strlen(name->data);
			if (pkt_node->alias_name_length > 0)
				memcpy(pkt_node->meta + pkt_node->node_name_length + pkt_node->relation_name_length + pkt_node->index_name_length, name->data, pkt_node->alias_name_length);
			resetStringInfo(name);
		}
	}
}

StringInfo
ExtractQual(PlanState *planstate, QueryDesc *qd, plan_tree_walker_context *context)
{
	if (!qd)
		return NULL;

	Plan       *plan      = planstate->plan;
	StringInfo  condition = makeStringInfo();

	switch nodeTag(plan)
	{
		case T_IndexScan:
		case T_BitmapIndexScan:
		{
			StringInfo condition1 = extract_scan_qual(
					((BitmapIndexScan *) plan)->indexqualorig,
					"Index Cond", planstate, context);
			AppendQualStringInfo(condition, condition1);
			break;
		}
		case T_BitmapHeapScan:
			/* FALL THRU */
		case T_SeqScan:
		case T_FunctionScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_WorkTableScan:
		case T_SubqueryScan:
		{
			StringInfo condition1 = extract_scan_qual(
					plan->qual, "Filter", planstate, context);
			AppendQualStringInfo(condition, condition1);
			break;
		}
		case T_TidScan:
		{
			StringInfo condition1 = NULL;
			StringInfo condition2 = NULL;

			List *tidquals = ((TidScan *) plan)->tidquals;
			if (list_length(tidquals) > 1)
				tidquals = list_make1(make_orclause(tidquals));
			condition1 = extract_scan_qual(tidquals, "TID Cond", planstate, context);
			condition2 = extract_scan_qual(plan->qual, "Filter", planstate, context);
			AppendQualStringInfo(condition, condition1);
			AppendQualStringInfo(condition, condition2);
			break;
		}
		case T_NestLoop:
		{
			StringInfo condition1 = NULL;
			StringInfo condition2 = NULL;

			condition1 = extract_upper_qual(
					((NestLoop *) plan)->join.joinqual,
					planstate, context, "Join Filter");
			condition2 = extract_upper_qual(plan->qual, planstate, context, "Filter");
			AppendQualStringInfo(condition, condition1);
			AppendQualStringInfo(condition, condition2);
			break;
		}
		case T_MergeJoin:
		{
			StringInfo condition1;
			StringInfo condition2;
			StringInfo condition3;
			condition1 = extract_upper_qual(
					((MergeJoin *) plan)->mergeclauses, planstate, context, "Merge Cond");
			condition2 = extract_upper_qual(
					((MergeJoin *) plan)->join.joinqual, planstate, context, "Join Filter");
			condition3 = extract_upper_qual(
					plan->qual, planstate, context, "Filter");
			AppendQualStringInfo(condition, condition1);
			AppendQualStringInfo(condition, condition2);
			AppendQualStringInfo(condition, condition3);
			break;
		}
		case T_HashJoin:
		{
			HashJoin *hash_join = (HashJoin *) plan;
			StringInfo condition1;
			StringInfo condition2;
			StringInfo condition3;
			/*
			 * In the case of an "IS NOT DISTINCT" condition, we display
			 * hashqualclauses instead of hashclauses.
			 */
			List *cond_to_show = hash_join->hashclauses;
			// FIXME:
			/*
			if (list_length(hash_join->hashqualclauses) > 0)
				cond_to_show = hash_join->hashqualclauses;
			*/
			condition1 = extract_upper_qual(
					cond_to_show, planstate, context, "Hash Cond");
			condition2 = extract_upper_qual(
					((HashJoin *) plan)->join.joinqual, planstate, context, "Join Filter");
			condition3 = extract_upper_qual(
					plan->qual, planstate, context, "Filter");
			AppendQualStringInfo(condition, condition1);
			AppendQualStringInfo(condition, condition2);
			AppendQualStringInfo(condition, condition3);
			break;
		}
		case T_Agg:
		{
			StringInfo condition1 = NULL;
			StringInfo condition2 = NULL;

			condition1 = extract_upper_qual(
					plan->qual, planstate, context, "Filter");
			condition2 = extract_grouping_keys(
					(AggState *) planstate, context, "Group By");
			AppendQualStringInfo(condition, condition1);
			AppendQualStringInfo(condition, condition2);
			break;
		}
		case T_Sort:
		{
			StringInfo condition1 = extract_sort_keys((SortState *) planstate, context);
			AppendQualStringInfo(condition, condition1);
			break;
		}
		case T_Result:
		{
			StringInfo condition1 = NULL;
			StringInfo condition2 = NULL;
			condition1 = extract_upper_qual(
					(List *)((Result *)plan)->resconstantqual,
					planstate, context, "One-Time Filter");
			condition2 = extract_upper_qual(
					plan->qual, planstate, context, "Filter");
			AppendQualStringInfo(condition, condition1);
			AppendQualStringInfo(condition, condition2);
			break;
		}
		default:
			break;
	}
	return condition;
}

StringInfo
extract_grouping_keys(AggState *aggstate, plan_tree_walker_context *context, const char *qlabel)
{
	Agg		*agg = (Agg *) aggstate->ss.ps.plan;
	List	*ctx;
	char	*exprstr;
	bool	first_flag = true;
	bool	useprefix;
	int		keyno;
	List	*ancestors;

	if (agg->numCols <= 0)
		return NULL;

	QueryDesc  *qd  = context->qd;
	StringInfo  res = makeStringInfo();
	appendStringInfo(res, "%s: ", qlabel);
	useprefix = (list_length(qd->plannedstmt->rtable) > 1);

	/* The key columns refer to the tlist of the child plan */
	ancestors = lcons(aggstate, context->ancestors);

	PlanState *lps     = outerPlanState(aggstate);
	Plan      *subplan = lps->plan;

	/* Set up deparsing context */
	ctx = set_deparse_context_planstate(context->deparse_cxt,
	                                    (Node *) lps,
	                                    context->ancestors);

	for (keyno = 0; keyno < agg->numCols; keyno++)
	{
		/* find key expression in tlist */
		AttrNumber      keyresno = agg->grpColIdx[keyno];
		TargetEntry    *target = get_tle_by_resno(subplan->targetlist, keyresno);

		if (target)
		{
			/* Deparse the expression, showing any top-level cast */
			exprstr = deparse_expr_for_qual((Node *) target->expr, ctx, useprefix, true);
			if (first_flag)
			{
				appendStringInfo(res, "%s", exprstr);
				first_flag = false;
			}
			else
			{
				appendStringInfo(res, ", %s", exprstr);
			}
		}
	}

	ancestors = list_delete_first(ancestors);
	return res;
}


// FIXME: gather_workfile_entries

List *
gather_workfile_entries(void)
{
	List                 *splist = NIL;
	/*
	int                   num_actives;
	int                   index;
	metrics_spill_file_t *sp;
	workfile_set         *work_sets;
	

	work_sets = workfile_mgr_cache_entries_get_copy(&num_actives);

	for (index = 0; index < num_actives; index++)
	{
		sp = (metrics_spill_file_t *) palloc0(sizeof(metrics_spill_file_t));

		if (work_sets[index].active)
		{
			splist = lappend(splist, sp);

			sp->workfile_size = work_sets[index].total_bytes;
			sp->tmid          = tmid;
			sp->sess_id       = work_sets[index].session_id;
			sp->ccnt          = work_sets[index].command_count;
			sp->slice_id      = work_sets[index].slice_id;
			sp->num_files     = work_sets[index].num_files;
		}
		else
			pfree(sp);
	}*/
	return splist;
}

// Should keep sync with ExplainPreScanNode
void
MetricsPreScanNode(PlanState *planstate, Bitmapset **rels_used)
{
	Plan   *plan = planstate->plan;

	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_WorkTableScan:
		case T_ForeignScan:
			*rels_used = bms_add_member(*rels_used,
			                            ((Scan *) plan)->scanrelid);
			break;
		case T_ModifyTable:
			/* cf ExplainModifyTarget */
			*rels_used = bms_add_member(*rels_used,
			          linitial_int(((ModifyTable *) plan)->resultRelations));
			break;
		default:
			break;
	}

	/* initPlan-s */
	if (planstate->initPlan)
		MetricsPreScanSubPlans(planstate->initPlan, rels_used);

	/* lefttree */
	if (outerPlanState(planstate))
		MetricsPreScanNode(outerPlanState(planstate), rels_used);

	/* righttree */
	if (innerPlanState(planstate))
		MetricsPreScanNode(innerPlanState(planstate), rels_used);

	/* special child plans */
	switch (nodeTag(plan))
	{
		case T_ModifyTable:
			MetricsPreScanMemberNodes(((ModifyTable *) plan)->plans,
			                      ((ModifyTableState *) planstate)->mt_plans,
			                          rels_used);
			break;
		case T_Append:
			MetricsPreScanMemberNodes(((Append *) plan)->appendplans,
			                        ((AppendState *) planstate)->appendplans,
			                          rels_used);
			break;
		case T_MergeAppend:
			MetricsPreScanMemberNodes(((MergeAppend *) plan)->mergeplans,
			                    ((MergeAppendState *) planstate)->mergeplans,
			                          rels_used);
			break;
		case T_BitmapAnd:
			MetricsPreScanMemberNodes(((BitmapAnd *) plan)->bitmapplans,
			                     ((BitmapAndState *) planstate)->bitmapplans,
			                          rels_used);
			break;
		case T_BitmapOr:
			MetricsPreScanMemberNodes(((BitmapOr *) plan)->bitmapplans,
			                      ((BitmapOrState *) planstate)->bitmapplans,
			                          rels_used);
			break;
		case T_SubqueryScan:
			MetricsPreScanNode(((SubqueryScanState *) planstate)->subplan,
			                   rels_used);
			break;
		default:
			break;
	}

	/* subPlan-s */
	if (planstate->subPlan)
		MetricsPreScanSubPlans(planstate->subPlan, rels_used);
}

// Should keep sync with ExplainPreScanMemberNodes
void
MetricsPreScanMemberNodes(List *plans, PlanState **planstates,
						  Bitmapset **rels_used)
{
	int			nplans = list_length(plans);
	int			j;

	for (j = 0; j < nplans; j++)
		MetricsPreScanNode(planstates[j], rels_used);
}

// Should keep sync with ExplainPreScanSubPlans
void
MetricsPreScanSubPlans(List *plans, Bitmapset **rels_used)
{
	ListCell   *lst;

	foreach(lst, plans)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lst);

		MetricsPreScanNode(sps->planstate, rels_used);
	}
}
