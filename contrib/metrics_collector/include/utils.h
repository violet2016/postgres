/*-------------------------------------------------------------------------
 *
 * utils.h
 *	  Definitions for query info struct and functions
 *
 * Copyright (c) 2017-Present Pivotal Software, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MC_UTILS_H
#define MC_UTILS_H

// Uncomment following line to print more debug messages
//#define MC_DEBUG_PRINT

#include "common.h"
#include "metrics_collector.h"

/* Interface */
extern void
AppendQualStringInfo(StringInfo condition, StringInfo append);
extern StringInfo
extract_scan_qual(List *qual, const char *qlabel, PlanState *planstate, plan_tree_walker_context *context);
extern StringInfo
extract_sort_keys(SortState *sortstate, plan_tree_walker_context *context);
extern StringInfo
extract_upper_qual(List *qual, PlanState *planstate, plan_tree_walker_context *context, const char *qlabel);
extern int
cut_valid_str_by_len(int encoding, const char *mbstr, int len);

/* Interface */
extern void
init_guc(void);
extern StringInfo
extract_grouping_keys(AggState *aggstate, plan_tree_walker_context *context, const char *qlabel);
extern const char*
ExtractNodeName(Plan *plan, QueryDesc *qd);
extern void
SetRelationInfo(Plan *plan, QueryDesc *qd, metrics_plan_init_node *pkt_node);
extern StringInfo
ExtractQual(PlanState *planstate, QueryDesc *qd, plan_tree_walker_context *context);
extern int32
mc_gettmid(void);

/* Inline functions */
#ifndef MC_INLINE
# if __GNUC__ && !__GNUC_STDC_INLINE__
#  define MC_INLINE extern inline
# else
#  define MC_INLINE inline
# endif
#endif

MC_INLINE bool
is_gpdb_master(void);
MC_INLINE bool
is_gpdb_master(void)
{
	return true;
}


#endif   /* MC_UTILS_H */
