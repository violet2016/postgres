/*-------------------------------------------------------------------------
 *
 * utils_gp.h
 *	  Definitions for query info struct and functions
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MC_UTILS_GP_H
#define MC_UTILS_GP_H

#include "utils.h"

extern void
MetricsPreScanNode(PlanState *planstate, Bitmapset **rels_used);
extern void
MetricsPreScanMemberNodes(List *plans, PlanState **planstates,
						  Bitmapset **rels_used);
extern void
MetricsPreScanSubPlans(List *plans, Bitmapset **rels_used);

extern int64
dispatch_udf_to_segDBs(const char *strCommand);

/*
 * Translate GPDB BackendState to GPCC BackendState. This is a safe
 * guard to not break GPCC when GPDB changed BackendState enum
 */
MC_INLINE void
populate_act_state(metrics_activity_t *act, PgBackendStatus *beentry);
MC_INLINE void
populate_act_state(metrics_activity_t *act, PgBackendStatus *beentry)
{
	switch(beentry->st_state)
	{
		case STATE_UNDEFINED:
			act->state = GPCC_STATE_UNKNOWN;
			break;
		case STATE_IDLE:
			act->state = GPCC_STATE_IDLE;
			break;
		case STATE_RUNNING:
			act->state = GPCC_STATE_RUNNING;
			break;
		case STATE_IDLEINTRANSACTION:
			act->state = GPCC_STATE_IDLEINTRANSACTION;
			break;
		case STATE_FASTPATH:
			act->state = GPCC_STATE_FASTPATH;
			break;
		case STATE_IDLEINTRANSACTION_ABORTED:
			act->state = GPCC_STATE_IDLEINTRANSACTION_ABORTED;
			break;
		case STATE_DISABLED:
			act->state = GPCC_STATE_DISABLED;
			break;
		default:
			break;
	}
}

MC_INLINE void
set_more_locktypes(metrics_lock_t *lr, uint8 locktype);
MC_INLINE void
set_more_locktypes(metrics_lock_t *lr, uint8 locktype)
{
}

MC_INLINE const char *
get_username(void);
MC_INLINE const char *
get_username(void)
{
	return GetConfigOption("session_authorization", false, false);
}

MC_INLINE bool
planstate_has_children(PlanState *ps);
MC_INLINE bool
planstate_has_children(PlanState *ps)
{
	return ps->initPlan ||
			outerPlanState(ps) ||
			innerPlanState(ps) ||
			IsA(ps->plan, ModifyTable) ||
			IsA(ps->plan, Append) ||
			IsA(ps->plan, MergeAppend) ||
			IsA(ps->plan, BitmapAnd) ||
			IsA(ps->plan, BitmapOr) ||
			IsA(ps->plan, SubqueryScan) ||
			ps->subPlan;
}

MC_INLINE void
init_deparse_cxt(plan_tree_walker_context *context, QueryDesc *qd);
MC_INLINE void
init_deparse_cxt(plan_tree_walker_context *context, QueryDesc *qd)
{
	Bitmapset  *rels_used = NULL;
	List       *rtable_names = NIL;

	MetricsPreScanNode(qd->planstate, &rels_used);
	rtable_names = select_rtable_names_for_explain(qd->plannedstmt->rtable, rels_used);
	context->deparse_cxt = deparse_context_for_plan_rtable(qd->plannedstmt->rtable, rtable_names);
}

MC_INLINE void
walk_children(PlanState *ps, Plan *plan, plan_tree_walker_context *context);
MC_INLINE void
walk_children(PlanState *ps, Plan *plan, plan_tree_walker_context *context)
{
	switch (nodeTag(plan))
	{
		case T_ModifyTable:
			emit_plan_node_walker_multi(((ModifyTable *) plan)->plans,
			                            ((ModifyTableState *) ps)->mt_plans,
			                            context);
			break;
		case T_MergeAppend:
			emit_plan_node_walker_multi(((MergeAppend *) plan)->mergeplans,
			                            ((MergeAppendState *) ps)->mergeplans,
			                            context);
			break;
		default:
			break;
	}
}

extern void
MetricsPreScanNode(PlanState *planstate, Bitmapset **rels_used);
extern void
MetricsPreScanMemberNodes(List *plans, PlanState **planstates,
						  Bitmapset **rels_used);
extern void
MetricsPreScanSubPlans(List *plans, Bitmapset **rels_used);

MC_INLINE void
populate_outer_plan(plan_tree_walker_context *context, Plan *plan);
MC_INLINE void
populate_outer_plan(plan_tree_walker_context *context, Plan *plan)
{
}

MC_INLINE List *
deparse_context_for_qual(PlanState *planstate, plan_tree_walker_context *context);
MC_INLINE List *
deparse_context_for_qual(PlanState *planstate, plan_tree_walker_context *context)
{
	return set_deparse_context_planstate(context->deparse_cxt,
	                                     (Node *) planstate,
	                                     context->ancestors);
}

MC_INLINE char *
deparse_expr_for_qual(Node *node, List *ctx, bool useprefix, bool showimplicit);
MC_INLINE char *
deparse_expr_for_qual(Node *node, List *ctx, bool useprefix, bool showimplicit)
{
	return deparse_expression(node, ctx, useprefix, showimplicit);
}
extern StringInfo
extract_motion_keys(PlanState *planstate, plan_tree_walker_context *context, const char *qlabel);

extern List*
gather_workfile_entries(void);

#endif   /* MC_UTILS_GP_H */
