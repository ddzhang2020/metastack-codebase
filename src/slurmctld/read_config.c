/*****************************************************************************\
 *  read_config.c - read the overall slurm configuration file
 *****************************************************************************
 *  Copyright (C) 2002-2007 The Regents of the University of California.
 *  Copyright (C) 2008-2010 Lawrence Livermore National Security.
 *  Copyright (C) SchedMD LLC.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Morris Jette <jette1@llnl.gov>.
 *  CODE-OCEC-09-009. All rights reserved.
 *
 *  This file is part of Slurm, a resource management program.
 *  For details, see <https://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  Slurm is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  Slurm is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with Slurm; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/

#include "config.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#ifdef  __METASTACK_OPT_GRES_CONFIG
#include <sys/time.h>
#endif

#include "src/common/assoc_mgr.h"
#include "src/common/cpu_frequency.h"
#include "src/common/hostlist.h"
#include "src/common/list.h"
#include "src/common/macros.h"
#include "src/common/port_mgr.h"
#include "src/common/read_config.h"
#include "src/common/slurm_rlimits_info.h"
#include "src/common/strnatcmp.h"
#include "src/common/xstring.h"
#ifdef  __METASTACK_OPT_GRES_CONFIG
#include "src/common/parse_config.h"
#endif

#include "src/interfaces/burst_buffer.h"
#include "src/interfaces/cgroup.h"
#include "src/interfaces/gres.h"
#include "src/interfaces/job_submit.h"
#include "src/interfaces/jobcomp.h"
#include "src/interfaces/mcs.h"
#include "src/interfaces/node_features.h"
#include "src/interfaces/preempt.h"
#include "src/interfaces/prep.h"
#include "src/interfaces/sched_plugin.h"
#include "src/interfaces/select.h"
#include "src/interfaces/switch.h"
#include "src/interfaces/topology.h"

#include "src/slurmctld/acct_policy.h"
#include "src/slurmctld/fed_mgr.h"
#include "src/slurmctld/front_end.h"
#include "src/slurmctld/gang.h"
#include "src/slurmctld/job_scheduler.h"
#include "src/slurmctld/licenses.h"
#include "src/slurmctld/locks.h"
#include "src/slurmctld/node_scheduler.h"
#include "src/slurmctld/power_save.h"
#include "src/slurmctld/proc_req.h"
#include "src/slurmctld/read_config.h"
#include "src/slurmctld/reservation.h"
#include "src/slurmctld/slurmctld.h"
#include "src/slurmctld/trigger_mgr.h"

#include "src/stepmgr/srun_comm.h"
#include "src/stepmgr/stepmgr.h"

#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
#include "src/common/proc_args.h"
#endif

#ifdef __METASTACK_NEW_RPC_RATE_LIMIT
#include "src/slurmctld/rate_limit.h"
#endif

#ifdef __METASTACK_OPT_APP
#include "src/common/xhash.h"
#include "src/common/fd.h"  
#include "src/slurmctld/state_save.h"  
#endif

#define FEATURE_MAGIC	0x34dfd8b5

/* Global variables */
List active_feature_list;	/* list of currently active features_records */
List avail_feature_list;	/* list of available features_records */
bool node_features_updated = true;
bool slurmctld_init_db = true;

#ifdef __METASTACK_OPT_MSG_OUTPUT
bool enable_reason_detail = false;
#endif

static void _acct_restore_active_jobs(void);
static void _add_config_feature(List feature_list, char *feature,
				bitstr_t *node_bitmap);
static void _add_config_feature_inx(List feature_list, char *feature,
				    int node_inx);
static void _build_bitmaps(void);
static void _gres_reconfig(void);
static void _init_all_slurm_conf(void);
static void _list_delete_feature(void *feature_entry);
static int _preserve_select_type_param(slurm_conf_t *ctl_conf_ptr,
                                       uint16_t old_select_type_p);
static int  _reset_node_bitmaps(void *x, void *arg);
static void _restore_job_accounting();

static void _set_features(node_record_t **old_node_table_ptr,
			  int old_node_record_count, int recover);
static void _stat_slurm_dirs(void);
static int  _sync_nodes_to_comp_job(void);
static int _sync_nodes_to_jobs(void);
static int  _sync_nodes_to_active_job(job_record_t *job_ptr);
static void _sync_nodes_to_suspended_job(job_record_t *job_ptr);
static void _sync_part_prio(void);
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
static uint16_t *_parse_pending_job_range(const char *str);
#endif

#ifdef __METASTACK_NEW_PART_PARA_SCHED
/* Global variables */
int part_count = 0;     /* The number of all partitions */
int resource_count = 0; /* The number of all partitions after resource area division  */
bool para_sched = false;    /* Flag for whether to execute parallel scheduling */
bool p_preemption_enabled = true; /* Flag for whether to execute preempt */
bool p_power_save_on = false; /* Flag for whether to execute power save */
/* for partitions */
char **para_sched_part_names = NULL;           /* A collection of partition names within each resource area, separated by spaces */
part_record_t ***original_part_ptr = NULL;     /* A collection of part_ptr for each part */
part_record_t ***para_sched_part_ptr = NULL;   /* A collection of part_ptr for all parts in each resource area */
/* for nodes */
bitstr_t **original_part_node_bitmap = NULL;    /* A collection of bitmaps for all nodes in each partition */
bitstr_t **para_sched_node_bitmap = NULL;       /* A collection of bitmaps for all nodes in each resource area */
bitstr_t **para_sched_avail_node_bitmap = NULL; /* A collection of bitmaps for available nodes in each resource area */
bitstr_t **para_sched_share_node_bitmap = NULL; /* A collection of bitmaps for sharable nodes in each resource area */
bitstr_t **para_sched_idle_node_bitmap = NULL;  /* A collection of bitmaps for idle nodes in each resource area */
#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_SUBMIT_PARALLEL
bool para_submit = false; 
bitstr_t **para_submit_avail_node_bitmap = NULL; /* A collection of bitmaps for available nodes in each resource area */
bitstr_t **para_submit_share_node_bitmap = NULL; /* A collection of bitmaps for sharable nodes in each resource area */
bitstr_t **para_submit_idle_node_bitmap = NULL;  /* A collection of bitmaps for idle nodes in each resource area */
#endif
#ifdef __METASTACK_NEW_MAIN_SCHED_PLANNED
bitstr_t **para_sched_main_planned_bitmap = NULL;
/* To be compatible with cache. */
bitstr_t **para_sched_last_main_planned_bitmap = NULL;
bitstr_t **para_sched_planned_update_bitmap = NULL;
#endif
#ifdef __METASTACK_NEW_HETPART_SUPPORT
bitstr_t **para_sched_resv_node_bitmap = NULL;
#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_SUBMIT_PARALLEL
bitstr_t **para_submit_resv_node_bitmap = NULL;
#endif
#endif

#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_EPILOG_PARALLEL
bool enable_para_epilog = false;
bitstr_t **para_epilog_cg_node_bitmap = NULL; /* A collection of bitmaps for completing nodes in each resource area */
bitstr_t **para_epilog_up_node_bitmap = NULL; /* A collection of bitmaps for up nodes, not DOWN in each resource area */
bitstr_t **para_epilog_avail_node_bitmap = NULL; /* A collection of bitmaps for available nodes in each resource area */
bitstr_t **para_epilog_bf_ignore_node_bitmap = NULL; /* A collection of bitmaps for nodes made available during backfill cycle in each resource area */
bitstr_t **para_epilog_idle_node_bitmap = NULL; /* A collection of bitmaps for idle nodes in each resource area */
#endif

#ifdef __METASTACK_BUG_PROCESS_DISTRIBUTION
bool disable_change_proc_dist = false;
#endif

#ifdef __METASTACK_OPT_APP    
List app_list = NULL;
time_t last_app_update = (time_t) 0;
char *default_app_name = NULL;
app_record_t *default_app_loc = NULL;
xhash_t *app_hash_table = NULL;
xhash_t *app_combined_hash = NULL;  /* secondary hash: "appname-version" → app_combined_entry_t */
#endif

#ifdef __METASTACK_BUG_OVERLAP_NODE_DIST
bool enable_overlap_node_lb = false;
#endif

/*
 * build_sched_resource - build the resource array required for parallel scheduling
 */
extern void build_sched_resource(void)
{
	int i, j, k;
	bool check_over = false;
	list_itr_t *part_iter = NULL;
	part_record_t *p_ptr = NULL;
	DEF_TIMERS;

	/* free all memory associated with para_sched */
	free_para_sched_resource();

	para_sched = false;	
	resource_count = 0;
	
	/* replace operation in func sort_job_queue2 */
	p_preemption_enabled = slurm_preemption_enabled();

	/* check whether config para_sched */
	if (!xstrcasestr(slurm_conf.sched_params, "para_sched")) 
		return;

	part_count = list_count(part_list);

	/* Verify whether resources meet parallel scheduling */
	if (part_count > 1) {
		START_TIMER;
		original_part_ptr = xcalloc(part_count, sizeof(part_record_t **));
		for(i = 0; i <part_count; i++) {
			*(original_part_ptr + i) = xcalloc(part_count , sizeof(part_record_t *));
		}
		original_part_node_bitmap = xcalloc(part_count, sizeof(bitstr_t *));

		part_iter = list_iterator_create(part_list);
		i = 0;

		/* original_part_ptr and original_part_node_bitmap are initialized  */
		while((p_ptr = list_next(part_iter))){
			original_part_ptr[i][0] = p_ptr;
			original_part_node_bitmap[i++] = bit_copy(p_ptr->node_bitmap);
		}
		list_iterator_destroy(part_iter);

		for (i = 0; i < part_count-1; i++, check_over = false) {
			for (j = i+1; j < part_count; j++) {
				if(original_part_node_bitmap[i] && original_part_node_bitmap[j] && 
					bit_overlap_any(original_part_node_bitmap[i], original_part_node_bitmap[j])) {
					/* rebuild original_part_node_bitmap[i] */
					bit_or(original_part_node_bitmap[i], original_part_node_bitmap[j]);
					if(original_part_ptr[j][0] != NULL){
						original_part_ptr[i][j] = original_part_ptr[j][0];
						original_part_ptr[j][0] = NULL;
						FREE_NULL_BITMAP(original_part_node_bitmap[j]);
						//original_part_node_bitmap[j] = NULL;
					}
				}
				if((j == part_count-1) && (!check_over)){
					check_over = true;
					j = i;
				}	
			}
		}
		/* Summary Results */
		for (i = 0; i < part_count; i++) {
			if(original_part_ptr[i][0] != NULL)
				resource_count++;
		}
		/* Resources meet the requirements of parallel scheduling */
		if(resource_count > 1){
			para_sched = true;
			debug("both configuration and partition resources meet parallel scheduling");
			debug("para_sched resource_count: %d", resource_count);

			para_sched_node_bitmap   = xcalloc(resource_count, sizeof(bitstr_t *));
			para_sched_avail_node_bitmap = xcalloc(resource_count, sizeof(bitstr_t *));
			para_sched_share_node_bitmap = xcalloc(resource_count, sizeof(bitstr_t *));
			para_sched_idle_node_bitmap  = xcalloc(resource_count, sizeof(bitstr_t *));
#ifdef __METASTACK_NEW_MAIN_SCHED_PLANNED
			para_sched_main_planned_bitmap = xcalloc(resource_count, sizeof(bitstr_t *));
			/* To be compatible with cache. */
			para_sched_last_main_planned_bitmap = xcalloc(resource_count, sizeof(bitstr_t *));
			para_sched_planned_update_bitmap = xcalloc(resource_count, sizeof(bitstr_t *));
#endif
#ifdef __METASTACK_NEW_HETPART_SUPPORT
			para_sched_resv_node_bitmap = xcalloc(resource_count, sizeof(bitstr_t *));
#endif

			/* build para_sched node bitmap */
			for (i = 0, j = 0; i < part_count && j < resource_count; i++) {
				if(original_part_node_bitmap[i] != NULL){
					para_sched_node_bitmap[j] = bit_copy(original_part_node_bitmap[i]);
#ifdef __METASTACK_NEW_MAIN_SCHED_PLANNED
					para_sched_main_planned_bitmap[j] = bit_alloc(node_record_count);
					/* To be compatible with cache. */
					para_sched_last_main_planned_bitmap[j] = bit_alloc(node_record_count);
					para_sched_planned_update_bitmap[j] = bit_alloc(node_record_count);
#endif
					/* doing other bitmap assignment during scheduling */
					/* para_sched_idle_node_bitmap */
					/* para_sched_avail_node_bitmap */
					/* para_sched_share_node_bitmap */

					/* debug("parallel scheduling region %d include nodes: %s",
							j, bitmap2node_name(original_part_node_bitmap[i])); */
					j++;	
				}
			}

			/* build para_sched part_record */
			para_sched_part_ptr = xcalloc(resource_count, sizeof(part_record_t **));
			for (i = 0; i < resource_count; i++) {
				*(para_sched_part_ptr + i) = xcalloc(part_count , sizeof(part_record_t *));
			}
			for (i = 0, k = 0; i < part_count && k < resource_count; i++) {
				if (original_part_ptr[i][0] == NULL)
					continue;
				for (j = 0; j < part_count && k < resource_count; j++) {
					if (original_part_ptr[i][j] != NULL)
						para_sched_part_ptr[k][j] = original_part_ptr[i][j];
				}
				k++;
			}
			get_para_sched_part_names();
		}
		END_TIMER;
		debug("time for %s was %s", __func__, TIME_STR);
	} 
}
#endif

#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
int list_find_watch_dog(void *x, void *key);
static void _init_watch_dog_record(watch_dog_record_t *watch_dog_ptr);
void init_watch_dog_conf(void);
static void _list_delete_watch_dog(void *watch_dog_entry);
#endif

#ifdef __METASTACK_OPT_APP  
static void _init_app_record(app_record_t *app_ptr);  
static void _list_delete_app(void *app_entry);  
static int _build_single_appline_info(app_record_t *app);  
static int _build_all_app_info(void);  
int list_find_app(void *x, void *key);  
#endif

/*
 * Setup the global response_cluster_rec
 */
static void _set_response_cluster_rec(void)
{
	if (response_cluster_rec)
		return;

	response_cluster_rec = xmalloc(sizeof(slurmdb_cluster_rec_t));
	response_cluster_rec->name = xstrdup(slurm_conf.cluster_name);
	if (slurm_conf.slurmctld_addr) {
		response_cluster_rec->control_host =
			xstrdup(slurm_conf.slurmctld_addr);
	} else {
		response_cluster_rec->control_host =
			xstrdup(slurm_conf.control_addr[0]);
	}
	response_cluster_rec->control_port = slurm_conf.slurmctld_port;
	response_cluster_rec->rpc_version = SLURM_PROTOCOL_VERSION;
}

/*
 * Free the global response_cluster_rec
 */
extern void cluster_rec_free(void)
{
	if (response_cluster_rec) {
		xfree(response_cluster_rec->control_host);
		xfree(response_cluster_rec->name);
		xfree(response_cluster_rec);
	}
}

/* Verify that Slurm directories are secure, not world writable */
static void _stat_slurm_dirs(void)
{
	struct stat stat_buf;
	char *problem_dir = NULL;

	/*
	 * PluginDir may have multiple values, and is checked by
	 * _is_valid_path() instead
	 */

	if (slurm_conf.plugstack &&
	    !stat(slurm_conf.plugstack, &stat_buf) &&
	    (stat_buf.st_mode & S_IWOTH)) {
		problem_dir = "PlugStack";
	}
	if (!stat(slurm_conf.slurmd_spooldir, &stat_buf) &&
	    (stat_buf.st_mode & S_IWOTH)) {
		problem_dir = "SlurmdSpoolDir";
	}
	if (!stat(slurm_conf.state_save_location, &stat_buf) &&
	    (stat_buf.st_mode & S_IWOTH)) {
		problem_dir = "StateSaveLocation";
	}

	if (problem_dir) {
		error("################################################");
		error("###       SEVERE SECURITY VULNERABILTY       ###");
		error("### %s DIRECTORY IS WORLD WRITABLE ###", problem_dir);
		error("###         CORRECT FILE PERMISSIONS         ###");
		error("################################################");
	}
}

/*
 * _reorder_nodes_by_rank - order node table in ascending order of node_rank
 * This depends on the TopologyPlugin, which may generate such a ranking.
 */
static int _sort_nodes_by_rank(const void *a, const void *b)
{
	node_record_t *n1 = *(node_record_t **)a;
	node_record_t *n2 = *(node_record_t **)b;

	if (!n1)
		return 1;
	if (!n2)
		return -1;

	return slurm_sort_uint32_list_asc(&n1->node_rank, &n2->node_rank);
}

/*
 * _reorder_nodes_by_name - order node table in ascending order of name
 */
static int _sort_nodes_by_name(const void *a, const void *b)
{
	node_record_t *n1 = *(node_record_t **)a;
	node_record_t *n2 = *(node_record_t **)b;

	if (!n1)
		return 1;
	if (!n2)
		return -1;

	return strnatcmp(n1->name, n2->name);
}

static void _sort_node_record_table_ptr(void)
{
	int (*compare_fn)(const void *, const void *);

	if (topology_g_generate_node_ranking())
		compare_fn = &_sort_nodes_by_rank;
	else
		compare_fn = &_sort_nodes_by_name;

	qsort(node_record_table_ptr, node_record_count,
	      sizeof(node_record_t *), compare_fn);

	for (int i = 0; i < node_record_count; i++) {
		if (node_record_table_ptr[i])
			node_record_table_ptr[i]->index = i;
	}

#if _DEBUG
	/* Log the results */
	node_record_t *node_ptr;
	for (int i = 0; (node_ptr = next_node(&i)); i++) {
		info("node_rank[%d:%d]: %s",
		     node_ptr->index, node_ptr->node_rank, node_ptr->name);
	}
#endif
}

static void _add_nodes_with_feature(hostlist_t *hl, char *feature)
{
	if (avail_feature_list) {
		char *feature_nodes;
		node_feature_t *node_feat_ptr;
		if (!(node_feat_ptr = list_find_first(avail_feature_list,
						      list_find_feature,
						      feature))) {
			debug2("unable to find nodeset feature '%s'", feature);
			return;
		}
		feature_nodes = bitmap2node_name(node_feat_ptr->node_bitmap);
		hostlist_push(hl, feature_nodes);
		xfree(feature_nodes);
	} else {
		node_record_t *node_ptr;
		/*
		 * The global feature bitmaps have not been set up at this
		 * point, so we'll have to scan through the node_record_table
		 * directly to locate the appropriate records.
		 */
		for (int i = 0; (node_ptr = next_node(&i)); i++) {
			char *features, *tmp, *tok, *last = NULL;

			if (!node_ptr->features)
				continue;

			features = tmp = xstrdup(node_ptr->features);

			while ((tok = strtok_r(tmp, ",", &last))) {
				if (!xstrcmp(tok, feature)) {
					hostlist_push_host(
						hl, node_ptr->name);
					break;
				}
				tmp = NULL;
			}
			xfree(features);
		}
	}
}

static void _add_all_nodes_to_hostlist(hostlist_t *hl)
{
	node_record_t *node_ptr;

	for (int i = 0; (node_ptr = next_node(&i)); i++)
		hostlist_push_host(hl, node_ptr->name);
}

extern hostlist_t *nodespec_to_hostlist(const char *nodes, bool uniq,
					char **nodesets)
{
	int count;
	slurm_conf_nodeset_t *ptr, **ptr_array;
	hostlist_t *hl;

	if (nodesets)
		xfree(*nodesets);

	if (!xstrcasecmp(nodes, "ALL")) {
		if (!(hl = hostlist_create(NULL))) {
			error("%s: hostlist_create() error for %s", __func__, nodes);
			return NULL;
		}
		_add_all_nodes_to_hostlist(hl);
		if (nodesets)
			*nodesets = xstrdup("ALL");
		return hl;
	} else if (!(hl = hostlist_create(nodes))) {
		error("%s: hostlist_create() error for %s", __func__, nodes);
		return NULL;
	}

	if (!hostlist_count(hl)) {
		/* no need to look for nodests */
		return hl;
	}

	count = slurm_conf_nodeset_array(&ptr_array);
	for (int i = 0; i < count; i++) {
		ptr = ptr_array[i];

		/* swap the nodeset entry with the applicable nodes */
		if (hostlist_delete_host(hl, ptr->name)) {
			if (nodesets)
				xstrfmtcat(*nodesets, "%s%s",
					   *nodesets ? "," : "",
					   ptr->name);

			if (ptr->feature)
				_add_nodes_with_feature(hl, ptr->feature);

			/* Handle keywords for Nodes= in a NodeSet */
			if (!xstrcasecmp(ptr->nodes, "ALL")) {
				_add_all_nodes_to_hostlist(hl);
			} else if (ptr->nodes) {
				hostlist_push(hl, ptr->nodes);
			}
		}
	}

	if (uniq)
		hostlist_uniq(hl);
	return hl;
}

#ifdef __METASTACK_BUG_PROCESS_DISTRIBUTION
static void init_disable_change_proc_dist(void) {
	if (xstrcasestr(slurm_conf.sched_params, "disable_change_proc_dist")) {
		disable_change_proc_dist = true;
	} else {
		disable_change_proc_dist = false;
	}
    debug("disable_change_proc_dist is %s", 
          disable_change_proc_dist ? "true" : "false");
}
#endif

static void _init_bitmaps(void)
{
	/* initialize the idle and up bitmaps */
	FREE_NULL_BITMAP(avail_node_bitmap);
	FREE_NULL_BITMAP(bf_ignore_node_bitmap);
	FREE_NULL_BITMAP(booting_node_bitmap);
	FREE_NULL_BITMAP(cg_node_bitmap);
	FREE_NULL_BITMAP(cloud_node_bitmap);
	FREE_NULL_BITMAP(future_node_bitmap);
	FREE_NULL_BITMAP(idle_node_bitmap);
	FREE_NULL_BITMAP(power_down_node_bitmap);
	FREE_NULL_BITMAP(power_up_node_bitmap);
	FREE_NULL_BITMAP(rs_node_bitmap);
	FREE_NULL_BITMAP(share_node_bitmap);
	FREE_NULL_BITMAP(up_node_bitmap);
#ifdef __METASTACK_NEW_HETPART_SUPPORT
	FREE_NULL_BITMAP(resv_node_bitmap);
	resv_node_bitmap = bit_alloc(node_record_count);
#endif 
	avail_node_bitmap = bit_alloc(node_record_count);
	bf_ignore_node_bitmap = bit_alloc(node_record_count);
	booting_node_bitmap = bit_alloc(node_record_count);
	cg_node_bitmap = bit_alloc(node_record_count);
	cloud_node_bitmap = bit_alloc(node_record_count);
	future_node_bitmap = bit_alloc(node_record_count);
	idle_node_bitmap = bit_alloc(node_record_count);
	power_down_node_bitmap = bit_alloc(node_record_count);
	power_up_node_bitmap = bit_alloc(node_record_count);
	rs_node_bitmap = bit_alloc(node_record_count);
	share_node_bitmap = bit_alloc(node_record_count);
	up_node_bitmap = bit_alloc(node_record_count);
}

#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
int node_borrow_interval = DEFAULT_NODE_BORROW_INTERVAL;


/* _parse_pending_job_range()
 *
 * Parse the pending job age/count range specified like min-max.
 *
 */
static uint16_t *_parse_pending_job_range(const char *str)
{
	char *min = NULL;
	char *max = NULL;
	char *dash = NULL;
	char *p = NULL;
	uint16_t *v = NULL;

	if (str == NULL || str[0] == '\0') {
		return NULL;
	}

	p = xstrdup(str);

	min = p;
	dash = xstrchr(p, '-');
	if (dash == NULL) {
		xfree(p);
		return NULL;
	}

	*dash = 0;
	max = dash + 1;

	v = xcalloc(2, sizeof(uint16_t));

	if (parse_uint16(min, &v[0]) || parse_uint16(max, &v[1]) || v[1] <= v[0]) {
		xfree(p);
		xfree(v);
	}

	xfree(p);	
	return v;
}

void valid_node_borrow_interval(void)
{
	char *tmp_ptr = NULL;
	int max_node_borrow_interval = 65533;
	if ((tmp_ptr = xstrcasestr(slurm_conf.slurmctld_params,
				"node_borrow_interval="))) {
		int tmp = atoi(tmp_ptr + 21);
		if (tmp <= 0 || tmp > max_node_borrow_interval) {
			error("node_borrow_interval configured invalid value, use default value");
			node_borrow_interval = DEFAULT_NODE_BORROW_INTERVAL;
		} else {
			node_borrow_interval = tmp;
		}
	} else {
		node_borrow_interval = DEFAULT_NODE_BORROW_INTERVAL;	
	}
	debug2("%s: node_borrow_interval: %d", __func__, node_borrow_interval);
}


/*
 * valid_standby_node_parameters - Confirm if the standby_node_parameters for partition 
 *  configuration are available
 */
extern int valid_standby_node_parameters(part_record_t *part_ptr)
{
	int rc = SLURM_SUCCESS;
	bool has_condition = false;

	if (!part_ptr->standby_nodes) {
		error("%s: partition %s failed to obtain structure standby_nodes, skip", __func__, part_ptr->name);
		return SLURM_ERROR;
	}
	
	if ((part_ptr->standby_nodes->nodes == NULL) || (part_ptr->standby_nodes->parameters == NULL)) {
		debug("%s: partition %s has incomplete standby_nodes information, skip", __func__, part_ptr->name);
		return SLURM_ERROR;
	} else {
		char *tmp_ptr = NULL, *tmp = NULL, *tok = NULL, *sep = NULL, *save_ptr = NULL;
		int nodes_can_borrow = 0, offline_node_state = 0;
		int job_min_age = 0, job_max_age = 0, job_min_count = 0, job_max_count = 0;
		uint16_t *job_range = NULL;

		tmp = xstrdup(part_ptr->standby_nodes->parameters);
		/* Check the range of nodes_can_borrow */
		if ((tmp_ptr = xstrcasestr(tmp, "nodes_can_borrow="))) {
			nodes_can_borrow = atoi(tmp_ptr + 17);
			if (nodes_can_borrow < 0 || nodes_can_borrow > MAX_NODES_CAN_BORROW) {
				error("%s: Invalid StandbyNodeParameters nodes_can_borrow: %d", __func__, nodes_can_borrow);
				xfree(tmp);
				return ESLURM_INVALID_STANDBY_NODE_PARAMETERS;
			}
		} else {
			error("%s: Invalid StandbyNodeParameters, not given nodes_can_borrow", __func__);
			xfree(tmp);
			return ESLURM_INVALID_STANDBY_NODE_PARAMETERS;
		}

		tok = strtok_r(tmp, ",", &save_ptr);
		while (tok) {
			sep = xstrchr(tok, '=');
			if (sep) {
				sep[0] = '\0';
				sep++;
				if (!xstrcasecmp(tok, "offline_node_state")) {
					if (!xstrcasecmp(sep, "drain")) {
						offline_node_state = 1;
						has_condition = true;
					} else if (!xstrcasecmp(sep, "down")) {
						offline_node_state = 2;
						has_condition = true;
					} else if (!xstrcasecmp(sep, "all")) {
						offline_node_state = 3;
						has_condition = true;
					} else {
						rc = ESLURM_INVALID_STANDBY_NODE_PARAMETERS;
						error("%s: Invalid StandbyNodeParameters offline_node_state: %s", __func__, sep);
					}
				} else if (!xstrcasecmp(tok, "pending_job_age_range")) {
					job_range = _parse_pending_job_range(sep);
					if (job_range) {
						job_min_age = job_range[0];
						job_max_age = job_range[1];
						has_condition = true;
						xfree(job_range);
					} else {
						rc = ESLURM_INVALID_STANDBY_NODE_PARAMETERS;
						error("%s: Invalid StandbyNodeParameters pending_job_age_range: %s", __func__, sep);
					}
				} else if (!xstrcasecmp(tok, "pending_job_count_range")) {				
					job_range = _parse_pending_job_range(sep);
					if (job_range) {
						job_min_count = job_range[0];
						job_max_count = job_range[1];
						has_condition = true;
						xfree(job_range);
					} else {
						rc = ESLURM_INVALID_STANDBY_NODE_PARAMETERS;
						error("%s: Invalid StandbyNodeParameters pending_job_count_range: %s", __func__, sep);
					}
				}
			} else {
				rc = ESLURM_INVALID_STANDBY_NODE_PARAMETERS;
				error("%s: Invalid StandbyNodeParameters: %s", __func__, tok);
			}

			if (rc == ESLURM_INVALID_STANDBY_NODE_PARAMETERS) {
				break;
			}

			tok = strtok_r(NULL, ",", &save_ptr);
		}
		xfree(tmp);

		if ((rc != SLURM_SUCCESS) || !has_condition) {
			return ESLURM_INVALID_STANDBY_NODE_PARAMETERS;
		}

		part_ptr->standby_nodes->nodes_can_borrow   = nodes_can_borrow;
		part_ptr->standby_nodes->offline_node_state = offline_node_state;
		part_ptr->standby_nodes->job_age_range[0]   = job_min_age;
		part_ptr->standby_nodes->job_age_range[1]   = job_max_age;
		part_ptr->standby_nodes->job_count_range[0] = job_min_count;
		part_ptr->standby_nodes->job_count_range[1] = job_max_count;
		
		return SLURM_SUCCESS;
	}
}
#endif

static void _build_part_bitmaps(void)
{
	part_record_t *part_ptr;
	list_itr_t *part_iterator;

	/* scan partition table and identify nodes in each */
	part_iterator = list_iterator_create(part_list);
	while ((part_ptr = list_next(part_iterator))) {
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
		if (build_part_bitmap(part_ptr, false) == ESLURM_INVALID_NODE_NAME)
			fatal("Invalid node names in partition %s",
					part_ptr->name);
		if ((!build_part_standby_nodes_bitmap(part_ptr)) && 
				(!valid_standby_node_parameters(part_ptr)) && 
				(part_ptr->standby_nodes->nodes_can_borrow > 0 )) {
			set_standby_nodes_flag(part_ptr);
		} else {
			if (part_ptr && part_ptr->standby_nodes) {
				part_ptr->standby_nodes->enable_for_job  = false;
				part_ptr->standby_nodes->enable_for_node = false;
			}
		}
		log_for_standby_nodes_flag(part_ptr);
#else
		if (build_part_bitmap(part_ptr) == ESLURM_INVALID_NODE_NAME)
			fatal("Invalid node names in partition %s",
					part_ptr->name);
#endif
	}
	list_iterator_destroy(part_iterator);
}

static void _build_node_config_bitmaps(void)
{
	node_record_t *node_ptr;

	/* initialize the configuration bitmaps */
	list_for_each(config_list, _reset_node_bitmaps, NULL);

	for (int i = 0; (node_ptr = next_node(&i)); i++) {
		if (node_ptr->config_ptr)
			bit_set(node_ptr->config_ptr->node_bitmap,
				node_ptr->index);
	}
}

static int _reset_node_bitmaps(void *x, void *arg)
{
	config_record_t *config_ptr = x;

	FREE_NULL_BITMAP(config_ptr->node_bitmap);
	config_ptr->node_bitmap = bit_alloc(node_record_count);

	return 0;
}

static int _set_share_node_bitmap(void *x, void *arg)
{
	job_record_t *job_ptr = x;

	if (!IS_JOB_RUNNING(job_ptr) ||
	    (job_ptr->node_bitmap == NULL)        ||
	    (job_ptr->details     == NULL)        ||
	    (job_ptr->details->share_res != 0))
		return 0;

	bit_and_not(share_node_bitmap, job_ptr->node_bitmap);

	return 0;
}

#ifndef HAVE_FRONT_END
static void *_set_node_addrs(void *arg)
{
	list_t *nodes = arg;
	slurm_addr_t slurm_addr;
	node_record_t *node_ptr;

	while ((node_ptr = list_pop(nodes))) {
		slurm_set_addr(&slurm_addr, node_ptr->port,
			       node_ptr->comm_name);
		if (slurm_get_port(&slurm_addr))
			continue;
		error("%s: failure on %s", __func__, node_ptr->comm_name);
		node_ptr->node_state = NODE_STATE_FUTURE;
		node_ptr->port = 0;
		xfree(node_ptr->reason);
		node_ptr->reason = xstrdup("NO NETWORK ADDRESS FOUND");
		node_ptr->reason_time = time(NULL);
		node_ptr->reason_uid = slurm_conf.slurm_user_id;
#ifdef __METASTACK_OPT_CACHE_QUERY
		_add_node_state_to_queue(node_ptr, true);
#endif
	}

	return NULL;
}
#endif

/*
 * Validate that nodes are addressable.
 */
static void _validate_slurmd_addr(void)
{
#ifndef HAVE_FRONT_END
	node_record_t *node_ptr;
	DEF_TIMERS;
	pthread_t *work_threads;
	int threads_num = 1;
	char *temp_str;
	list_t *nodes = list_create(NULL);
	xassert(verify_lock(CONF_LOCK, READ_LOCK));

	START_TIMER;

	if ((temp_str = xstrcasestr(slurm_conf.slurmctld_params,
				    "validate_nodeaddr_threads="))) {
		int tmp_val = strtol(temp_str + 26, NULL, 10);
		if ((tmp_val >= 1) && (tmp_val <= 64))
			threads_num = tmp_val;
		else
			error("SlurmctldParameters option validate_nodeaddr_threads=%d out of range, ignored",
			      tmp_val);
	}


	for (int i = 0; (node_ptr = next_node(&i)); i++) {
		if ((node_ptr->name == NULL) ||
		    (node_ptr->name[0] == '\0'))
			continue;
		if (IS_NODE_FUTURE(node_ptr))
			continue;
		if (IS_NODE_CLOUD(node_ptr) &&
		    (IS_NODE_POWERING_DOWN(node_ptr) ||
		     IS_NODE_POWERED_DOWN(node_ptr) ||
		     IS_NODE_POWERING_UP(node_ptr)))
				continue;
		if (node_ptr->port == 0)
			node_ptr->port = slurm_conf.slurmd_port;
		list_append(nodes, node_ptr);
	}

	work_threads = xcalloc(threads_num, sizeof(pthread_t));
	for (int i = 0; i < threads_num; i++)
		slurm_thread_create(&work_threads[i], _set_node_addrs, nodes);
	for (int i = 0; i < threads_num; i++)
		slurm_thread_join(work_threads[i]);
	xfree(work_threads);
	xassert(list_is_empty(nodes));
	FREE_NULL_LIST(nodes);

	END_TIMER2(__func__);
#endif
}

/*
 * _build_bitmaps - build node bitmaps to define which nodes are in which
 *    1) partition  2) configuration record  3) up state  4) idle state
 *    also sets values of total_nodes and total_cpus for every partition.
 * RET 0 if no error, errno otherwise
 * Note: Operates on common variables, no arguments
 *	node_record_count - number of nodes in the system
 *	node_record_table_ptr - pointer to global node table
 *	part_list - pointer to global partition list
 */
static void _build_bitmaps(void)
{
	node_record_t *node_ptr;

	last_node_update = time(NULL);
	last_part_update = time(NULL);

	/* Set all bits, all nodes initially available for sharing */
	bit_set_all(share_node_bitmap);

	/* identify all nodes non-sharable due to non-sharing jobs */
	list_for_each(job_list, _set_share_node_bitmap, NULL);

	/* scan all nodes and identify which are up, idle and
	 * their configuration, resync DRAINED vs. DRAINING state */
	for (int i = 0; (node_ptr = next_node(&i)); i++) {
		uint32_t drain_flag, job_cnt;

		if (node_ptr->name[0] == '\0')
			continue;	/* defunct */
		drain_flag = IS_NODE_DRAIN(node_ptr) |
			     IS_NODE_FAIL(node_ptr);
		job_cnt = node_ptr->run_job_cnt + node_ptr->comp_job_cnt;
		if (!IS_NODE_FUTURE(node_ptr))
			bit_set(power_up_node_bitmap, node_ptr->index);

		if ((IS_NODE_IDLE(node_ptr) && (job_cnt == 0)) ||
		    IS_NODE_DOWN(node_ptr))
			bit_set(idle_node_bitmap, node_ptr->index);
		if (IS_NODE_POWERING_UP(node_ptr))
			bit_set(booting_node_bitmap, node_ptr->index);
		if (IS_NODE_COMPLETING(node_ptr))
			bit_set(cg_node_bitmap, node_ptr->index);
		if (IS_NODE_CLOUD(node_ptr))
			bit_set(cloud_node_bitmap, node_ptr->index);
		if (IS_NODE_IDLE(node_ptr) ||
		    IS_NODE_ALLOCATED(node_ptr) ||
		    ((IS_NODE_REBOOT_REQUESTED(node_ptr) ||
		      IS_NODE_REBOOT_ISSUED(node_ptr)) &&
		     ((node_ptr->next_state & NODE_STATE_FLAGS) &
		      NODE_RESUME))) {
			if ((drain_flag == 0) &&
			    (!IS_NODE_NO_RESPOND(node_ptr)))
#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_EPILOG_PARALLEL
				make_node_avail(node_ptr, false, 0);
#else
				make_node_avail(node_ptr);
#endif
			bit_set(up_node_bitmap, node_ptr->index);
		}
		if (IS_NODE_POWERED_DOWN(node_ptr)) {
			bit_set(power_down_node_bitmap, node_ptr->index);
			bit_clear(power_up_node_bitmap, node_ptr->index);
		}
		if (IS_NODE_POWERING_DOWN(node_ptr)) {
			bit_set(power_down_node_bitmap, node_ptr->index);
			bit_clear(power_up_node_bitmap, node_ptr->index);
			bit_clear(avail_node_bitmap, node_ptr->index);
		}
		if (IS_NODE_FUTURE(node_ptr))
			bit_set(future_node_bitmap, node_ptr->index);

		if ((IS_NODE_REBOOT_REQUESTED(node_ptr) ||
		     IS_NODE_REBOOT_ISSUED(node_ptr)) &&
		    ((node_ptr->next_state & NODE_STATE_FLAGS) & NODE_RESUME))
			bit_set(rs_node_bitmap, node_ptr->index);
	}
}

#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
static void _list_delete_watch_dog(void *watch_dog_entry)
{
	watch_dog_record_t *watch_dog_ptr;
	watch_dog_ptr = (watch_dog_record_t *) watch_dog_entry;
	xfree(watch_dog_ptr->watch_dog);
	xfree(watch_dog_ptr->script);
	xfree(watch_dog_ptr->describe);
	xfree(watch_dog_ptr->account);
	xfree(watch_dog_ptr);
}

/*
 * Create a global watch dog list.
 *
 * This should be called before creating any watch dog entries.
 */
void init_watch_dog_conf(void) 
{
	last_watch_dog_update = time(NULL);
	if (watch_dog_list)		/* delete defunct partitions */
		list_flush(watch_dog_list);
	else
		watch_dog_list = list_create(_list_delete_watch_dog);
}
#endif

#ifdef __METASTACK_OPT_APP  
/*    
 * xhash helper: index app_record by app_name in app_hash_table (primary).    
 */    
static void _app_record_hash_identity(void *item, const char **key,    
				      uint32_t *key_len)    
{    
	app_record_t *app_ptr = (app_record_t *)item;    
	*key = app_ptr->app_name;    
	*key_len = strlen(app_ptr->app_name);    
}  
  
/*    
 * app_combined_entry_t - Lightweight wrapper for the secondary hash.    
 * Maps "appname-version" to the owning app_record_t.    
 * The combined_name string is owned by this entry.    
 */    
typedef struct {    
	char         *combined_name;  /* "appname-version", owned */    
	app_record_t *app_ptr;        /* back-pointer, non-owning */    
} app_combined_entry_t;  
  
static void _app_combined_hash_identity(void *item, const char **key,    
					uint32_t *key_len)    
{    
	app_combined_entry_t *e = (app_combined_entry_t *)item;    
	*key = e->combined_name;    
	*key_len = strlen(e->combined_name);    
}  
  
static void _app_combined_entry_free(void *item)    
{    
	app_combined_entry_t *e = (app_combined_entry_t *)item;    
	if (!e)
		return;
	xfree(e->combined_name);    
	xfree(e);    
}
  
/*    
 * _rebuild_combined_hash_for_app - Build secondary hash entries for one app.    
 * If versions is NULL (no version restriction), creates a single entry    
 * with key = app_name. Otherwise creates one entry per version.    
 * This hash enables O(1) lookup by combined name (e.g., "vasp-5.7.1").    
 * IN app_ptr - app record to rebuild hash for    
 */    
static void _rebuild_combined_hash_for_app(app_record_t *app_ptr)  
{  
	if (!app_ptr || !app_ptr->app_name)  
		return;  

	if (!app_ptr->versions || !app_ptr->versions[0]) {  
		/* No version restriction: key = app_name itself */  
		app_combined_entry_t *existing;  
		existing = (app_combined_entry_t *)xhash_get_str(  
				app_combined_hash, app_ptr->app_name);  
		if (existing) {  
			/* Hash key collision - skip this entry to avoid data corruption */  
			error("%s: combined hash key collision: "  
			      "app \"%s\" (no version) collides with "  
			      "app \"%s\", key=\"%s\". Skipping.",  
			      __func__, app_ptr->app_name,  
			      existing->app_ptr->app_name,  
			      app_ptr->app_name);  
			return;  
		}  
		app_combined_entry_t *e = xmalloc(sizeof(*e));  
		e->combined_name = xstrdup(app_ptr->app_name);  
		e->app_ptr = app_ptr;  
		xhash_add(app_combined_hash, e);  
		return;  
	}  

	/* Iterate comma-separated versions and create hash entry for each */  
	char *copy = xstrdup(app_ptr->versions);  
	char *save_ptr = NULL;  
	char *tok = strtok_r(copy, ",", &save_ptr);  
	while (tok) {
		/* Trim leading whitespace */
		while (*tok == ' ' || *tok == '\t')
			tok++;
		if (*tok == '\0') {
			tok = strtok_r(NULL, ",", &save_ptr);
			continue;
		}
		/* Trim trailing whitespace */
		char *end = tok + strlen(tok) - 1;
		while (end > tok && (*end == ' ' || *end == '\t'))
			*end-- = '\0';
  
		if (*tok) {  
			char *combined_key = NULL;  
			app_combined_entry_t *existing;  

			/* Build combined key: "app_name-version" */  
			xstrfmtcat(combined_key, "%s-%s",  
				   app_ptr->app_name, tok);  

			/* Check for hash key collision before inserting */  
			existing = (app_combined_entry_t *)xhash_get_str(  
					app_combined_hash, combined_key);  
			if (existing) {  
				error("%s: combined hash key collision: "  
				      "app \"%s\" version \"%s\" collides "  
				      "with app \"%s\", key=\"%s\". "  
				      "Skipping.",  
				      __func__, app_ptr->app_name, tok,  
				      existing->app_ptr->app_name,  
				      combined_key);  
				xfree(combined_key);  
			} else {  
				/* Create and insert new hash entry */  
				app_combined_entry_t *e =  
					xmalloc(sizeof(*e));  
				e->combined_name = combined_key;  
				e->app_ptr = app_ptr;  
				xhash_add(app_combined_hash, e);  
			}  
		}  
		tok = strtok_r(NULL, ",", &save_ptr);  
	}  
	xfree(copy);  
}

/*    
 * _remove_version_from_list - Remove a single version from a    
 *   comma-separated versions string.    
 * IN/OUT versions_ptr - pointer to versions string (may be reallocated)    
 * IN ver - version to remove    
 */    
static void _remove_version_from_list(char **versions_ptr, const char *ver)      
{      
	char *versions, *new_versions = NULL;      
	char *copy, *save_ptr = NULL, *tok;      
  
	if (!versions_ptr || !*versions_ptr || !ver)      
		return;      
  
	versions = *versions_ptr;      
	copy = xstrdup(versions);      
	tok = strtok_r(copy, ",", &save_ptr);
	while (tok) {
		while (*tok == ' ' || *tok == '\t')
			tok++;
		if (*tok == '\0') {
			tok = strtok_r(NULL, ",", &save_ptr);
			continue;
		}
		char *end = tok + strlen(tok) - 1;
		while (end > tok && (*end == ' ' || *end == '\t'))
			*end-- = '\0';
		if (xstrcmp(tok, ver) != 0) {      
			if (new_versions)      
				xstrfmtcat(new_versions, ",%s", tok);      
			else      
				new_versions = xstrdup(tok);      
		}      
		tok = strtok_r(NULL, ",", &save_ptr);      
	}      
	xfree(copy);      
	xfree(*versions_ptr);      
	*versions_ptr = new_versions;      
}

/*    
 * _version_in_list - Check if a version string is in a comma-separated list.    
 * IN versions - comma-separated version list (e.g. "5.7.1,5.7.2,6.0")    
 * IN ver      - version to find    
 * RET true if found    
 */    
static bool _version_in_list(const char *versions, const char *ver)    
{    
	if (!versions || !ver)    
		return false;    
	char *copy = xstrdup(versions);    
	char *save_ptr = NULL;    
	char *tok = strtok_r(copy, ",", &save_ptr);
	while (tok) {
		while (*tok == ' ' || *tok == '\t')
			tok++;
		if (*tok == '\0') {
			tok = strtok_r(NULL, ",", &save_ptr);
			continue;
		}
		char *end = tok + strlen(tok) - 1;
		while (end > tok && (*end == ' ' || *end == '\t'))
			*end-- = '\0';
		if (!xstrcmp(tok, ver)) {    
			xfree(copy);    
			return true;    
		}    
		tok = strtok_r(NULL, ",", &save_ptr);    
	}    
	xfree(copy);    
	return false;    
}  

/*  
 * _app_versions_add - Add comma-separated versions to app's version list.  
 *   Skips duplicates. ver_copy tokens are "+"-prefixed.  
 * IN app_ptr - app record to modify  
 * IN ver_copy - version string with "+" prefix (e.g., "+5.9.0,+5.9.1")  
 * IN app_name - app name for logging  
 */  
static void _app_versions_add(app_record_t *app_ptr, char *ver_copy,  
                               const char *app_name)  
{  
	char *save_ptr = NULL;  
	char *tok = strtok_r(ver_copy, ",", &save_ptr);  
	while (tok) {  
		/* Skip the "+" prefix */  
		if (*tok == '+')  
			tok++;  
		/* Trim leading whitespace */  
		while (*tok == ' ' || *tok == '\t')  
			tok++;  
		if (*tok != '\0') {  
			/* Check if version already exists to avoid duplicates */  
			if (!_version_in_list(app_ptr->versions, tok)) {  
				if (app_ptr->versions &&  
				    app_ptr->versions[0])  
					/* Append to existing list */  
					xstrfmtcat(app_ptr->versions,  
						   ",%s", tok);  
				else {  
					/* First version, replace entire string */  
					xfree(app_ptr->versions);  
					app_ptr->versions = xstrdup(tok);  
				}  
				info("App version added: %s-%s",  
				     app_name, tok);  
			} else {  
				info("%s: version '%s' already exists "  
				     "for AppName=%s, skipping",  
				     __func__, tok, app_name);  
			}  
		}  
		tok = strtok_r(NULL, ",", &save_ptr);  
	}  
}  
  
/*  
 * _app_versions_remove - Remove comma-separated versions from app's  
 *   version list. ver_copy tokens are "-"-prefixed.  
 * IN app_ptr - app record to modify  
 * IN ver_copy - version string with "-" prefix (e.g., "-5.7.1,-3.7.1")  
 * IN app_name - app name for logging  
 */  
static void _app_versions_remove(app_record_t *app_ptr, char *ver_copy,  
                                  const char *app_name)  
{  
	char *save_ptr = NULL;  
	char *tok = strtok_r(ver_copy, ",", &save_ptr);  
	while (tok) {  
		/* Skip the "-" prefix */  
		if (*tok == '-')  
			tok++;  
		/* Trim leading whitespace */  
		while (*tok == ' ' || *tok == '\t')  
			tok++;  
		if (*tok != '\0') {  
			/* Only remove if version exists in the list */  
			if (_version_in_list(app_ptr->versions, tok)) {  
				_remove_version_from_list(  
					&app_ptr->versions, tok);  
				info("App version removed: %s-%s",  
				     app_name, tok);  
			} else {  
				info("%s: version '%s' not found "  
				     "for AppName=%s, skipping",  
				     __func__, tok, app_name);  
			}  
		}  
		tok = strtok_r(NULL, ",", &save_ptr);  
	}  
}  
  
/*  
 * _app_versions_replace - Replace app's entire version list.  
 * Used when Version string has no +/- prefix (complete replacement).  
 * IN app_ptr - app record to modify  
 * IN new_versions - new version string (e.g., "5.7.1,5.7.2,5.8.0")  
 */  
static void _app_versions_replace(app_record_t *app_ptr,  
                                   const char *new_versions)  
{  
	/* Free old version string and replace with new one */  
	xfree(app_ptr->versions);  
	app_ptr->versions = xstrdup(new_versions);  
}

/*    
 * _remove_combined_hash_for_app - Remove all secondary hash entries for one app.    
 */    
static void _remove_combined_hash_for_app(app_record_t *app_ptr)    
{    
	if (!app_ptr || !app_ptr->app_name)    
		return;    
  
	if (!app_ptr->versions || !app_ptr->versions[0]) {    
		xhash_delete_str(app_combined_hash, app_ptr->app_name);    
		return;    
	}    
  
	char *copy = xstrdup(app_ptr->versions);    
	char *save_ptr = NULL;    
	char *tok = strtok_r(copy, ",", &save_ptr);
	while (tok) {
		while (*tok == ' ' || *tok == '\t')
			tok++;
		if (*tok == '\0') {
			tok = strtok_r(NULL, ",", &save_ptr);
			continue;
		}
		char *end = tok + strlen(tok) - 1;
		while (end > tok && (*end == ' ' || *end == '\t'))
			*end-- = '\0';
		if (*tok) {    
			char *buf = NULL;    
			xstrfmtcat(buf, "%s-%s", app_ptr->app_name, tok);    
			xhash_delete_str(app_combined_hash, buf);    
			xfree(buf);    
		}    
		tok = strtok_r(NULL, ",", &save_ptr);    
	}    
	xfree(copy);    
}  
  
static void _list_delete_app(void *app_entry)    
{    
	app_record_t *app_ptr = (app_record_t *)app_entry;    
	xfree(app_ptr->app_name);    
	xfree(app_ptr->versions);    
	xfree(app_ptr->description);    
	xfree(app_ptr->watchdog);    
	xfree(app_ptr);    
}    
  
/*    
 * init_app_conf - Reset app subsystem to empty state.    
 *    
 * Called during initial config load and before rebuilding from state file.    
 * Order matters: free combined hash first (owns entry wrappers), then    
 * free main hash (non-owning references), then flush list (which frees    
 * the actual app_record_t memory via _list_delete_app).    
 */  
void init_app_conf(void)    
{    
	last_app_update = time(NULL);    
	xfree(default_app_name);    
	default_app_loc = NULL;    
  
	/* Clear combined hash first (owns entry wrappers) */    
	xhash_free(app_combined_hash);    
	app_combined_hash = xhash_init(_app_combined_hash_identity,    
				       _app_combined_entry_free);    
  
	/* Clear main hash (non-owning references) */    
	xhash_free(app_hash_table);    
	app_hash_table = xhash_init(_app_record_hash_identity, NULL);    
  
	if (app_list)    
		list_flush(app_list);    
	else    
		app_list = list_create(_list_delete_app);    
}  
    
void app_fini(void)    
{    
	xhash_free(app_combined_hash);    
	xhash_free(app_hash_table);    
	FREE_NULL_LIST(app_list);    
	xfree(default_app_name);    
	default_app_loc = NULL;    
}  
    
static void _init_app_record(app_record_t *app_ptr)    
{    
	app_ptr->app_name = NULL;    
	app_ptr->versions = NULL;    
	app_ptr->description = NULL;    
	app_ptr->watchdog = NULL;    
	app_ptr->default_flag = false;    
}  
    
/*    
 * list_find_app - find an entry in the app list by app_name.    
 * IN key - pointer to app_name string    
 * RET 1 if matches, 0 otherwise    
 */    
int list_find_app(void *x, void *key)    
{    
	app_record_t *app_ptr = (app_record_t *)x;    
	char *find_name = (char *)key;    
	return (!xstrcmp(app_ptr->app_name, find_name));    
}    
  
/*    
 * create_app_record - Create a new app record and add to list + hash tables.    
 * IN name     - application name (unique primary key)    
 * IN versions - comma-separated version list, or NULL for no restriction    
 * RET pointer to new app record    
 */    
app_record_t *create_app_record(const char *name, const char *versions)    
{    
	app_record_t *app_ptr = xmalloc(sizeof(*app_ptr));    
  
	last_app_update = time(NULL);    
  
	_init_app_record(app_ptr);    
	app_ptr->app_name = xstrdup(name);    
	if (versions && versions[0])    
		app_ptr->versions = xstrdup(versions);    
  
	(void)list_append(app_list, app_ptr);    
	xhash_add(app_hash_table, app_ptr);    
  
	/* Build secondary hash entries for --app= validation */    
	_rebuild_combined_hash_for_app(app_ptr);    
  
	return app_ptr;    
}  
  
/*    
 * find_app_record - find an app record by app_name (primary hash, O(1)).    
 * IN app_name - application name    
 * RET pointer to app record or NULL if not found    
 */    
app_record_t *find_app_record(const char *app_name)    
{    
	if (!app_name || !app_hash_table)    
		return NULL;    
	return (app_record_t *)xhash_get_str(app_hash_table, app_name);    
}  
  
/*    
 * find_app_record_by_combined - find an app record by combined name    
 *   (e.g. "vasp-5.7.1") using secondary hash for O(1) lookup.    
 *   Also supports bare app_name (e.g. "vasp") when versions is NULL.    
 * IN combined_name - e.g. "vasp-5.7.1" or "vasp"    
 * RET pointer to app record or NULL if not found    
 */    
app_record_t *find_app_record_by_combined(const char *combined_name)    
{    
	app_combined_entry_t *e;    
  
	if (!combined_name || !app_combined_hash)    
		return NULL;    
  
	e = (app_combined_entry_t *)xhash_get_str(app_combined_hash,    
						  combined_name);    
	return e ? e->app_ptr : NULL;    
}
  
/*    
 * _build_single_appline_info - Process one AppName config line.    
 *    
 * If an app with the same app_name already exists, merge: append new    
 * versions to the existing versions list, and overwrite description/    
 * watchdog/default with the latest values.    
 * If not found, create a new record.    
 * IN app - app record from config file (may be merged into existing)    
 * RET SLURM_SUCCESS on success    
 */    
static int _build_single_appline_info(app_record_t *app)      
{      
	app_record_t *app_ptr = NULL;  

	/* Use primary hash for O(1) duplicate detection by app_name */      
	app_ptr = (app_record_t *)xhash_get_str(app_hash_table,    
						app->app_name);    

	if (app_ptr) {      
		/* Same AppName already exists — merge versions, overwrite props */    
		info("%s: AppName=%s specified more than once, merging",    
		     __func__, app->app_name);  

		/* Merge versions: append new versions to existing list */    
		if (app->versions && app->versions[0]) {    
			/* Remove old combined hash entries before changing versions */    
			_remove_combined_hash_for_app(app_ptr);    

			if (app_ptr->versions && app_ptr->versions[0]) {  
				/* Append only versions not already present */  
				char *copy = xstrdup(app->versions);  
				char *save_ptr = NULL;  
				char *tok = strtok_r(copy, ",", &save_ptr);
				while (tok) {
					/* Trim leading whitespace */
					while (*tok == ' ' || *tok == '\t')
						tok++;
					if (*tok == '\0') {
						tok = strtok_r(NULL, ",", &save_ptr);
						continue;
					}
					/* Trim trailing whitespace */
					char *end = tok + strlen(tok) - 1;
					while (end > tok && (*end == ' ' || *end == '\t'))
						*end-- = '\0';
  
					/* Check for duplicate and append if new */  
					if (*tok && !_version_in_list(  
							app_ptr->versions, tok)) {  
						xstrfmtcat(app_ptr->versions,  
								",%s", tok);  
					}  
					tok = strtok_r(NULL, ",", &save_ptr);  
				}  
				xfree(copy);  
			} else {  
				/* No existing versions, replace entirely */  
				xfree(app_ptr->versions);  
				app_ptr->versions = xstrdup(app->versions);  
			} 

			/* Rebuild combined hash entries with updated versions */    
			_rebuild_combined_hash_for_app(app_ptr);    
		}    

		/* Overwrite description if provided */    
		if (app->description) {    
			xfree(app_ptr->description);    
			app_ptr->description = xstrdup(app->description);    
		}    

		/* Overwrite watchdog if provided */    
		if (app->watchdog) {    
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION      
			/* Validate watchdog reference before setting */  
			if (list_find_first(watch_dog_list,    
					    &list_find_watch_dog,    
					    app->watchdog)) {      
				xfree(app_ptr->watchdog);    
				app_ptr->watchdog = xstrdup(app->watchdog);      
			} else {      
				error("AppName=%s references undefined "    
				      "Watchdog '%s', ignoring watchdog setting",    
				      app->app_name, app->watchdog);      
			}      
#else      
			xfree(app_ptr->watchdog);    
			app_ptr->watchdog = xstrdup(app->watchdog);    
#endif      
		}    

		/* Handle default flag — clear old default if changing */    
		if (app->default_flag) {    
			if (default_app_name &&    
			    default_app_loc != app_ptr) {    
				info("%s: changing default app from %s to %s",    
				     __func__, default_app_name,    
				     app->app_name);    
				default_app_loc->default_flag = false;    
			}    
			app_ptr->default_flag = true;    
			xfree(default_app_name);    
			default_app_name = xstrdup(app_ptr->app_name);    
			default_app_loc = app_ptr;    
		}      

		return 0;    
	}    

	/* New AppName — create record */    
	app_ptr = create_app_record(app->app_name, app->versions);      

	/* Set optional metadata fields */  
	if (app->description)      
		app_ptr->description = xstrdup(app->description);      

	if (app->watchdog) {      
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION      
		/* Validate watchdog reference before setting */  
		if (list_find_first(watch_dog_list, &list_find_watch_dog,      
				    app->watchdog)) {      
			app_ptr->watchdog = xstrdup(app->watchdog);      
		} else {      
			error("AppName=%s references undefined "    
			      "Watchdog '%s', ignoring watchdog setting",    
			      app->app_name, app->watchdog);      
		}      
#else      
		app_ptr->watchdog = xstrdup(app->watchdog);    
#endif      
	}      

	app_ptr->default_flag = app->default_flag;      

	/* Handle default flag for new app */  
	if (app->default_flag) {    
		if (default_app_name &&    
		    xstrcmp(default_app_loc->app_name, app->app_name)) {    
			info("%s: changing default app from %s to %s",    
			     __func__,    
			     default_app_loc->app_name,    
			     app->app_name);    
		}    
		xfree(default_app_name);    
		default_app_name = xstrdup(app->app_name);    
		default_app_loc = app_ptr;    
	}      

	return 0;      
}  
  
/*    
 * _build_all_app_info - Build app records from slurm.conf AppName lines.    
 * Each config line becomes one app_record_t with AppName as unique key.    
 * Version is stored as-is (comma-separated list).    
 */    
static int _build_all_app_info(void)      
{      
	app_record_t **app_array = NULL;      
	int count = 0;      
	int i;      
  
	count = slurm_conf_app_array(&app_array);      
	if (count == 0) {      
		debug("No AppName information available");      
		return SLURM_ERROR;      
	}      
  
	for (i = 0; i < count; i++) {      
		app_record_t *app = app_array[i];      
		_build_single_appline_info(app);      
	}      
  
	return SLURM_SUCCESS;      
}

#define APP_STATE_VERSION "METASTACK_APP_STATE_001"  
  
typedef struct {    
	buf_t *buffer;    
	uint32_t apps_packed;    
	uint16_t protocol_version;    
	uid_t uid;    
} _foreach_pack_app_info_t;  

void pack_app(app_record_t *app_ptr, buf_t *buffer,  
              uint16_t protocol_version)  
{  
#ifdef __META_PROTOCOL  
	if (protocol_version >= META_3_2_PROTOCOL_VERSION) {  
		packstr(app_ptr->app_name, buffer);  
		packstr(app_ptr->versions, buffer);  
		packstr(app_ptr->description, buffer);  
		packstr(app_ptr->watchdog, buffer);  
		packbool(app_ptr->default_flag, buffer);  
	}  
#endif  
}
  
static int _pack_app(void *object, void *arg)    
{    
	app_record_t *app_ptr = object;    
	_foreach_pack_app_info_t *pack_info = arg;    
	int offset_before = get_buf_offset(pack_info->buffer);    
	pack_app(app_ptr, pack_info->buffer, pack_info->protocol_version);    
	if (get_buf_offset(pack_info->buffer) != offset_before)    
		pack_info->apps_packed++;    
	return SLURM_SUCCESS;    
}

/*  
 * dump_all_app_state - save the state of all app records to file  
 *   for later recovery upon restart.  
 * RET 0 or error code  
 */  
extern int dump_all_app_state(void)  
{  
	int error_code = 0, log_fd;  
	char *old_file, *new_file, *reg_file;  
	slurmctld_lock_t app_read_lock = {  
		READ_LOCK, NO_LOCK, NO_LOCK, NO_LOCK, NO_LOCK };  
	buf_t *buffer = init_buf(BUF_SIZE);  
	DEF_TIMERS;  
  
	START_TIMER;  
  
	/* write header */  
	packstr(APP_STATE_VERSION, buffer);  
	pack16(SLURM_PROTOCOL_VERSION, buffer);  
	pack_time(time(NULL), buffer);  
  
	/* write app records to buffer */  
	lock_slurmctld(app_read_lock);  
	if (app_list) {  
		list_itr_t *iter = list_iterator_create(app_list);  
		app_record_t *app_ptr;  
		while ((app_ptr = list_next(iter)))  
			pack_app(app_ptr, buffer, SLURM_PROTOCOL_VERSION);  
		list_iterator_destroy(iter);  
	}  
	unlock_slurmctld(app_read_lock);  
  
	/* write the buffer to file */  
	old_file = xstrdup(slurm_conf.state_save_location);  
	xstrcat(old_file, "/app_state.old");  
	reg_file = xstrdup(slurm_conf.state_save_location);  
	xstrcat(reg_file, "/app_state");  
	new_file = xstrdup(slurm_conf.state_save_location);  
	xstrcat(new_file, "/app_state.new");  
  
	lock_state_files();  
	log_fd = creat(new_file, 0600);  
	if (log_fd < 0) {  
		error("Can't save state, error creating file %s, %m",  
		      new_file);  
		error_code = errno;  
	} else {  
		int pos = 0, nwrite = get_buf_offset(buffer), amount, rc;  
		char *data = (char *)get_buf_data(buffer);  
  
		while (nwrite > 0) {  
			amount = write(log_fd, &data[pos], nwrite);  
			if ((amount < 0) && (errno != EINTR)) {  
				error("Error writing file %s, %m", new_file);  
				error_code = errno;  
				break;  
			}  
			nwrite -= amount;  
			pos    += amount;  
		}  
		rc = fsync_and_close(log_fd, "app");  
		if (rc && !error_code)  
			error_code = rc;  
	}  
	if (error_code)  
		(void)unlink(new_file);  
	else {	/* file shuffle */  
		(void)unlink(old_file);  
		if (link(reg_file, old_file))  
			debug4("unable to create link for %s -> %s: %m",  
			       reg_file, old_file);  
		(void)unlink(reg_file);  
		if (link(new_file, reg_file))  
			debug4("unable to create link for %s -> %s: %m",  
			       new_file, reg_file);  
		(void)unlink(new_file);  
	}  
	xfree(old_file);  
	xfree(reg_file);  
	xfree(new_file);  
	unlock_state_files();  
  
	FREE_NULL_BUFFER(buffer);  
	END_TIMER2(__func__);  
	return error_code;  
}

/* Open the app state save file, or backup if necessary. */  
static buf_t *_open_app_state_file(char **state_file)  
{  
	buf_t *buf;  
  
	*state_file = xstrdup(slurm_conf.state_save_location);  
	xstrcat(*state_file, "/app_state");  
	if (!(buf = create_mmap_buf(*state_file)))  
		error("Could not open app state file %s: %m",  
		      *state_file);  
	else  
		return buf;  
  
	error("NOTE: Trying backup app state save file. "  
	      "App config changes may be lost");  
	xstrcat(*state_file, ".old");  
	return create_mmap_buf(*state_file);  
}

/*
 * load_all_app_state - load the app state from file, recover on
 *	slurmctld restart. execute this after loading the configuration
 *	file data. Consistent with load_all_part_state().
 *
 * Note: reads dump from dump_all_app_state().
 */
extern int load_all_app_state(uint16_t reconfig_flags)
{
	char *state_file = NULL, *ver_str = NULL;
	char *app_name = NULL, *versions = NULL;
	char *description = NULL, *watchdog = NULL;
	bool default_flag = false;
	time_t time;
	int error_code = 0, app_count = 0;
	buf_t *buffer;
	uint16_t protocol_version = NO_VAL16;
	app_record_t *app_ptr;

	xassert(verify_lock(CONF_LOCK, READ_LOCK));

	if (!(reconfig_flags & RECONFIG_KEEP_APP_INFO)) {
		debug("Restoring app state from state file disabled");
		return SLURM_SUCCESS;
	}

	/* read the file */
	lock_state_files();
	if (!(buffer = _open_app_state_file(&state_file))) {
		info("No app state file (%s) to recover",
		     state_file);
		xfree(state_file);
		unlock_state_files();
		return ENOENT;
	}
	xfree(state_file);
	unlock_state_files();

	safe_unpackstr(&ver_str, buffer);
	debug3("Version string in app_state header is %s", ver_str);
	if (ver_str && !xstrcmp(ver_str, APP_STATE_VERSION))
		safe_unpack16(&protocol_version, buffer);

	if (protocol_version == NO_VAL16) {
		if (!ignore_state_errors)
			fatal("Can not recover app state, data version incompatible, start with '-i' to ignore this. Warning: using -i will lose the data that can't be recovered.");
		error("*****************************************************");
		error("Can not recover app state, data version incompatible");
		error("*****************************************************");
		xfree(ver_str);
		FREE_NULL_BUFFER(buffer);
		return EFAULT;
	}
	xfree(ver_str);
	safe_unpack_time(&time, buffer);

	while (remaining_buf(buffer) > 0) {
		app_name = NULL;
		versions = NULL;
		description = NULL;
		watchdog = NULL;
		default_flag = false;

#ifdef __META_PROTOCOL
		if (protocol_version >= META_3_2_PROTOCOL_VERSION) {
			safe_unpackstr(&app_name, buffer);
			safe_unpackstr(&versions, buffer);
			safe_unpackstr(&description, buffer);
			safe_unpackstr(&watchdog, buffer);
			safe_unpackbool(&default_flag, buffer);
		} else {
			goto unpack_error;
		}
#else
		goto unpack_error;
#endif

		/* validity test as possible */
		if (!app_name || !app_name[0]) {
			error("%s: skipping state record with empty AppName",
			      __func__);
			error_code = EINVAL;
		}
		if (error_code) {
			error("No more app data will be processed from "
			      "the checkpoint file");
			xfree(app_name);
			xfree(versions);
			xfree(description);
			xfree(watchdog);
			error_code = EINVAL;
			break;
		}

		/* find record and perform update */
		app_ptr = find_app_record(app_name);
		if (!app_ptr && (reconfig_flags & RECONFIG_KEEP_APP_INFO)) {
			info("%s: app %s missing from configuration "
			     "file, creating from state",
			     __func__, app_name);
			app_ptr = create_app_record(app_name, versions);
		} else if (!app_ptr) {
			info("%s: app %s removed from configuration "
			     "file, skipping",
			     __func__, app_name);
		}

		if (app_ptr) {
			app_count++;

			if (versions) {
				_remove_combined_hash_for_app(app_ptr);
				xfree(app_ptr->versions);
				app_ptr->versions = versions;
				versions = NULL;
				_rebuild_combined_hash_for_app(app_ptr);
			}

			xfree(app_ptr->description);
			app_ptr->description = description;
			description = NULL;

			xfree(app_ptr->watchdog);
			app_ptr->watchdog = watchdog;
			watchdog = NULL;

			app_ptr->default_flag = default_flag;
			if (default_flag) {
				xfree(default_app_name);
				default_app_name = xstrdup(app_ptr->app_name);
				default_app_loc = app_ptr;
			}
		}

		xfree(app_name);
		xfree(versions);
		xfree(description);
		xfree(watchdog);
	}

	info("Recovered state of %d app records", app_count);
	FREE_NULL_BUFFER(buffer);
	return error_code;

unpack_error:
	if (!ignore_state_errors)
		fatal("Incomplete app data checkpoint file, start with '-i' to ignore this. Warning: using -i will lose the data that can't be recovered.");
	error("Incomplete app data checkpoint file");
	xfree(app_name);
	xfree(versions);
	xfree(description);
	xfree(watchdog);
	info("Recovered state of %d app records", app_count);
	FREE_NULL_BUFFER(buffer);
	return EFAULT;
}

/*  
 * pack_all_app — Serialize all app records for RESPONSE_BUILD_APP_INFO.  
 *  
 * Called by slurmctld when handling "scontrol show app" requests.  
 * Format: [record_count (uint32)] [timestamp] [app_record]*  
 * Each app_record is packed by pack_app().  
 *  
 * IN uid              - requesting user (reserved for future ACL use)  
 * IN protocol_version - RPC protocol version for pack format selection  
 * RET buf_t containing the serialized app info message  
 */
extern buf_t *pack_all_app(uid_t uid, uint16_t protocol_version)  
{  
	time_t now = time(NULL);  
	int tmp_offset = 0;  
	_foreach_pack_app_info_t pack_app_info = {  
		.buffer = init_buf(BUF_SIZE),  
		.apps_packed = 0,  
		.protocol_version = protocol_version,  
		.uid = uid,  
	};  
  
	pack32(0, pack_app_info.buffer);  
	pack_time(now, pack_app_info.buffer);  
  
	if (app_list)  
		list_for_each_ro(app_list, _pack_app, &pack_app_info);  
  
	tmp_offset = get_buf_offset(pack_app_info.buffer);  
	set_buf_offset(pack_app_info.buffer, 0);  
	pack32(pack_app_info.apps_packed, pack_app_info.buffer);  
	set_buf_offset(pack_app_info.buffer, tmp_offset);  
  
	return pack_app_info.buffer;  
}

/*    
 * update_app - Create or update an app record (scontrol RPC handler).    
 *    
 * IN app_desc   - App description from RPC message.    
 * IN create_flag - true=create new, false=update existing.    
 * RET SLURM_SUCCESS or ESLURM_* error code.    
 *    
 * On create: validates uniqueness by app_name, creates record.    
 * On update: modifies versions/description/watchdog/default of existing record.    
 * Version supports +/- prefix for add/remove operations.    
 * Both paths: update last_app_update timestamp and schedule state file save.    
 *    
 * Default app management: at most one app can be default. Setting a new    
 * default clears the old one. default_app_name (string) and default_app_loc    
 * (pointer) are always kept in sync.    
 */  
extern int update_app(app_desc_msg_t *app_desc, bool create_flag)        
{        
	app_record_t *app_ptr = NULL;        
      
	if (!app_desc->app_name || !app_desc->app_name[0]) {        
		info("%s: missing AppName", __func__);        
		return ESLURM_INVALID_APP_NAME;        
	}        
      
	if (create_flag) {      
		/* --- CREATE path: app_name must be unique --- */      
    
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION      
		if (app_desc->watchdog && app_desc->watchdog[0]) {      
			if (!list_find_first(watch_dog_list,      
			                     &list_find_watch_dog,      
			                     app_desc->watchdog)) {      
				info("%s: AppName=%s references "      
				     "undefined Watchdog '%s'",      
				     __func__, app_desc->app_name,      
				     app_desc->watchdog);      
				return ESLURM_INVALID_APP_WATCHDOG;      
			}      
		}      
#endif      
      
		app_ptr = find_app_record(app_desc->app_name);      
		if (app_ptr) {      
			info("%s: App '%s' already exists",      
			     __func__, app_desc->app_name);      
			return ESLURM_APP_ALREADY_EXISTS;      
		}      
		app_ptr = create_app_record(app_desc->app_name,      
		                            app_desc->versions);      
		if (!app_ptr)      
			return SLURM_ERROR;      
      
		if (app_desc->description)      
			app_ptr->description = xstrdup(app_desc->description);      
		if (app_desc->watchdog)      
			app_ptr->watchdog = xstrdup(app_desc->watchdog);      
		if (app_desc->default_spec == APP_DESC_DEFAULT_YES) {      
			if (default_app_loc && default_app_loc != app_ptr)      
				default_app_loc->default_flag = false;      
			app_ptr->default_flag = true;      
			xfree(default_app_name);      
			default_app_name = xstrdup(app_ptr->app_name);      
			default_app_loc = app_ptr;      
		}      
		last_app_update = time(NULL);      
		schedule_app_save();      
		info("App created: %s", app_ptr->app_name);      
    
	} else if (app_desc->versions && app_desc->versions[0]) {      
		/* --- UPDATE path with Version specified --- */      
		/*      
		 * Version string may contain +/- prefixed tokens from      
		 * scontrol_process_plus_minus(), e.g.:      
		 *   "+6.0.0,+6.1.0"  → add versions      
		 *   "-4.7.1,-3.7.1"  → remove versions      
		 *   "5.7.1,5.7.2"    → replace entire versions list      
		 */      
		char *ver_copy = xstrdup(app_desc->versions);      
		bool has_plus = (ver_copy[0] == '+' || strstr(ver_copy, ",+") != NULL);  
		bool has_minus = (ver_copy[0] == '-' || strstr(ver_copy, ",-") != NULL);    
    
		if (has_plus && has_minus) {      
			info("%s: cannot mix + and - in Version for AppName=%s",      
			     __func__, app_desc->app_name);      
			xfree(ver_copy);      
			return ESLURM_INVALID_APP_NAME;      
		}      
    
		app_ptr = find_app_record(app_desc->app_name);      
		if (!app_ptr) {      
			info("%s: App '%s' not found",      
			     __func__, app_desc->app_name);      
			xfree(ver_copy);      
			return ESLURM_APP_NOT_FOUND;      
		}      
    
		/* Validate watchdog BEFORE modifying versions (atomicity) */      
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION      
		if (app_desc->watchdog && app_desc->watchdog[0]) {      
			if (!list_find_first(watch_dog_list,      
			                     &list_find_watch_dog,      
			                     app_desc->watchdog)) {      
				info("%s: AppName=%s references undefined "      
				     "Watchdog '%s'",      
				     __func__, app_desc->app_name,      
				     app_desc->watchdog);      
				xfree(ver_copy);      
				return ESLURM_INVALID_APP_WATCHDOG;      
			}      
		}      
#endif      
    
		/* Remove old combined hash entries before modifying versions */      
		_remove_combined_hash_for_app(app_ptr);      
  
		if (has_plus)  
			_app_versions_add(app_ptr, ver_copy, app_desc->app_name);  
		else if (has_minus)  
			_app_versions_remove(app_ptr, ver_copy, app_desc->app_name);  
		else  
			_app_versions_replace(app_ptr, app_desc->versions);  
    
		/* Rebuild combined hash entries with updated versions */      
		_rebuild_combined_hash_for_app(app_ptr);      
    
		xfree(ver_copy);      
    
		/* Also update other properties if provided */      
		if (app_desc->description) {      
			xfree(app_ptr->description);      
			app_ptr->description =      
				xstrdup(app_desc->description);      
		}      
    
		if (app_desc->watchdog) {      
			xfree(app_ptr->watchdog);      
			app_ptr->watchdog =      
				xstrdup(app_desc->watchdog);      
		}      
    
		if (app_desc->default_spec !=      
		    APP_DESC_DEFAULT_IGNORE) {      
			bool new_default =      
				(app_desc->default_spec ==      
				 APP_DESC_DEFAULT_YES);      
			if (new_default && !app_ptr->default_flag) {      
				if (default_app_loc &&      
				    default_app_loc != app_ptr)      
					default_app_loc->default_flag =      
						false;      
				app_ptr->default_flag = true;      
				xfree(default_app_name);      
				default_app_name =      
					xstrdup(app_ptr->app_name);      
				default_app_loc = app_ptr;      
			} else if (!new_default &&      
				   app_ptr->default_flag) {      
				app_ptr->default_flag = false;      
				if (default_app_loc == app_ptr) {      
					xfree(default_app_name);      
					default_app_loc = NULL;      
				}      
			}      
		}      
    
		last_app_update = time(NULL);      
		schedule_app_save();      
		info("App updated: %s", app_ptr->app_name);      
    
	} else {      
		/* --- No Version specified: update properties only --- */      
		app_ptr = find_app_record(app_desc->app_name);      
		if (!app_ptr) {      
			info("%s: App '%s' not found",      
			     __func__, app_desc->app_name);      
			return ESLURM_APP_NOT_FOUND;      
		}      
    
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION      
		if (app_desc->watchdog && app_desc->watchdog[0]) {      
			if (!list_find_first(watch_dog_list,      
			                     &list_find_watch_dog,      
			                     app_desc->watchdog)) {      
				info("%s: undefined Watchdog '%s'",      
				     __func__, app_desc->watchdog);      
				return ESLURM_INVALID_APP_WATCHDOG;      
			}      
		}      
#endif      
    
		if (app_desc->description) {      
			xfree(app_ptr->description);      
			app_ptr->description =      
				xstrdup(app_desc->description);      
		}      
		if (app_desc->watchdog) {      
			xfree(app_ptr->watchdog);      
			app_ptr->watchdog =      
				xstrdup(app_desc->watchdog);      
		}      
		if (app_desc->default_spec !=      
		    APP_DESC_DEFAULT_IGNORE) {      
			bool new_default =      
				(app_desc->default_spec ==      
				 APP_DESC_DEFAULT_YES);      
			if (new_default && !app_ptr->default_flag) {      
				if (default_app_loc &&      
				    default_app_loc != app_ptr)      
					default_app_loc->default_flag =      
						false;      
				app_ptr->default_flag = true;      
				xfree(default_app_name);      
				default_app_name =      
					xstrdup(app_ptr->app_name);      
				default_app_loc = app_ptr;      
			} else if (!new_default &&      
				   app_ptr->default_flag) {      
				app_ptr->default_flag = false;      
				if (default_app_loc == app_ptr) {      
					xfree(default_app_name);      
					default_app_loc = NULL;      
				}      
			}      
		}      
    
		last_app_update = time(NULL);      
		schedule_app_save();      
		info("App updated: %s", app_ptr->app_name);      
	}      
      
	return SLURM_SUCCESS;        
}


/*    
 * delete_app - Remove an app record by app_name.    
 *    
 * Deletion order: remove combined hash entries first, then remove from    
 * primary hash table (O(1), non-owning), then remove from list (which    
 * triggers _list_delete_app to free memory).    
 */  
extern int delete_app(delete_app_msg_t *app_msg)    
{    
	app_record_t *app_ptr;    
    
	if (!app_msg->name || !app_msg->name[0]) {    
		info("%s: missing app name", __func__);    
		return ESLURM_APP_NOT_FOUND;    
	}    
    
	app_ptr = find_app_record(app_msg->name);    
	if (!app_ptr) {    
		info("%s: App '%s' not found", __func__, app_msg->name);    
		return ESLURM_APP_NOT_FOUND;    
	}    
    
	if (app_ptr->default_flag && default_app_loc == app_ptr) {    
		xfree(default_app_name);    
		default_app_loc = NULL;    
	}    
    
	/* Remove secondary hash entries first */    
	_remove_combined_hash_for_app(app_ptr);    
  
	/* Remove from primary hash table (O(1)) */    
	xhash_pop_str(app_hash_table, app_ptr->app_name);    
  
	/* Then delete from list (triggers _list_delete_app to free memory) */    
	list_delete_first(app_list, &list_find_app, app_ptr->app_name);    
	last_app_update = time(NULL);  
	schedule_app_save();  
    
	info("App deleted: %s", app_msg->name);    
	return SLURM_SUCCESS;    
}  
#endif /* __METASTACK_OPT_APP */

/*
 * _init_all_slurm_conf - initialize or re-initialize the slurm
 *	configuration values.
 * NOTE: We leave the job table intact
 * NOTE: Operates on common variables, no arguments
 */
static void _init_all_slurm_conf(void)
{
	char *conf_name = xstrdup(slurm_conf.slurm_conf);

	slurm_conf_reinit(conf_name);
	xfree(conf_name);

	init_node_conf();
	init_part_conf();
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
	init_watch_dog_conf();
#endif
#ifdef __METASTACK_OPT_APP  
	init_app_conf();  
#endif 
	init_job_conf();
}

static int _handle_downnodes_line(slurm_conf_downnodes_t *down)
{
	int error_code = 0;
	node_record_t *node_rec = NULL;
	hostlist_t *alias_list = NULL;
	char *alias = NULL;
	int state_val = NODE_STATE_DOWN;

	if (down->state != NULL) {
		state_val = state_str2int(down->state, down->nodenames);
		if (state_val == NO_VAL) {
			error("Invalid State \"%s\"", down->state);
			goto cleanup;
		}
	}

	if ((alias_list = hostlist_create(down->nodenames)) == NULL) {
		error("Unable to create NodeName list from %s",
		      down->nodenames);
		error_code = errno;
		goto cleanup;
	}

	while ((alias = hostlist_shift(alias_list))) {
		node_rec = find_node_record(alias);
		if (node_rec == NULL) {
			error("DownNode \"%s\" does not exist!", alias);
			free(alias);
			continue;
		}

		if ((state_val != NO_VAL) &&
		    (state_val != NODE_STATE_UNKNOWN)){
			node_rec->node_state = state_val;
#ifdef __METASTACK_OPT_CACHE_QUERY
			_add_node_state_to_queue(node_rec, true);
#endif
		}
		if (down->reason) {
			xfree(node_rec->reason);
			node_rec->reason = xstrdup(down->reason);
			node_rec->reason_time = time(NULL);
			node_rec->reason_uid = slurm_conf.slurm_user_id;
		}
		free(alias);
	}

cleanup:
	if (alias_list)
		hostlist_destroy(alias_list);
	return error_code;
}

static void _handle_all_downnodes(void)
{
	slurm_conf_downnodes_t *ptr, **ptr_array;
	int count;
	int i;

	count = slurm_conf_downnodes_array(&ptr_array);
	if (count == 0) {
		debug("No DownNodes");
		return;
	}

	for (i = 0; i < count; i++) {
		ptr = ptr_array[i];

		_handle_downnodes_line(ptr);
	}
}

/*
 * Convert a comma delimited string of account names into a List containing
 * pointers to those associations.
 */
extern list_t *accounts_list_build(char *accounts, bool locked)
{
	char *tmp_accts, *one_acct_name, *name_ptr = NULL;
	list_t *acct_list = NULL;
	slurmdb_assoc_rec_t *assoc_ptr = NULL;
	assoc_mgr_lock_t locks = { .assoc = READ_LOCK };

	if (!accounts)
		return acct_list;

	if (!locked)
		assoc_mgr_lock(&locks);
	tmp_accts = xstrdup(accounts);
	one_acct_name = strtok_r(tmp_accts, ",", &name_ptr);
	while (one_acct_name) {
		slurmdb_assoc_rec_t assoc = {
			.acct = one_acct_name,
			.uid = NO_VAL,
		};

		if (assoc_mgr_fill_in_assoc(
			    acct_db_conn, &assoc,
			    accounting_enforce,
			    &assoc_ptr, true) != SLURM_SUCCESS) {
			if (accounting_enforce & ACCOUNTING_ENFORCE_ASSOCS) {
				error("%s: No association for account %s",
				      __func__, assoc.acct);
			} else {
				verbose("%s: No association for account %s",
					__func__, assoc.acct);
			}

		}
		if (assoc_ptr) {
			if (!acct_list)
				acct_list = list_create(NULL);
			list_append(acct_list, assoc_ptr);
		}

		one_acct_name = strtok_r(NULL, ",", &name_ptr);
	}
	xfree(tmp_accts);
	if (!locked)
		assoc_mgr_unlock(&locks);
	return acct_list;
}

/* Convert a comma delimited list of QOS names into a bitmap */
extern void qos_list_build(char *qos, bitstr_t **qos_bits)
{
	char *tmp_qos, *one_qos_name, *name_ptr = NULL;
	slurmdb_qos_rec_t qos_rec, *qos_ptr = NULL;
	bitstr_t *tmp_qos_bitstr;
	int rc;
	assoc_mgr_lock_t locks = { .qos = READ_LOCK };

	if (!qos) {
		FREE_NULL_BITMAP(*qos_bits);
		return;
	}

	/* Lock here to avoid g_qos_count changing under us */
	assoc_mgr_lock(&locks);
	if (!g_qos_count) {
		error("We have no QOS on the system Ignoring invalid "
		      "Allow/DenyQOS value(s) %s",
		      qos);
		assoc_mgr_unlock(&locks);
		FREE_NULL_BITMAP(*qos_bits);
		*qos_bits = NULL;
		return;
	}

	tmp_qos_bitstr = bit_alloc(g_qos_count);
	tmp_qos = xstrdup(qos);
	one_qos_name = strtok_r(tmp_qos, ",", &name_ptr);
	while (one_qos_name) {
		memset(&qos_rec, 0, sizeof(slurmdb_qos_rec_t));
		qos_rec.name = one_qos_name;
		rc = assoc_mgr_fill_in_qos(acct_db_conn, &qos_rec,
					   accounting_enforce,
					   &qos_ptr, 1);
		if ((rc != SLURM_SUCCESS) || (qos_rec.id >= g_qos_count)) {
			error("Ignoring invalid Allow/DenyQOS value: %s",
			      one_qos_name);
		} else {
			bit_set(tmp_qos_bitstr, qos_rec.id);
		}
		one_qos_name = strtok_r(NULL, ",", &name_ptr);
	}
	assoc_mgr_unlock(&locks);
	xfree(tmp_qos);
	FREE_NULL_BITMAP(*qos_bits);
	*qos_bits = tmp_qos_bitstr;
}

#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
/*
 * Sync with _init_conf_part().
 *
 * _init_conf_part() initializes default values from slurm.conf parameters.
 * After parsing slurm.conf, _build_single_partitionline_info() copies
 * slurm_conf_partition_t to part_record_t. Default values between
 * slurm_conf_partition_t and part_record_t should stay in sync in case a
 * part_record_t is created outside of slurm.conf parsing.
 */
static void _init_watch_dog_record(watch_dog_record_t *watch_dog_ptr)
{
	watch_dog_ptr->init_time = 0;
	watch_dog_ptr->period = 0;
	watch_dog_ptr->show_flags = 0;
	watch_dog_ptr->enable_all_nodes = false;
	watch_dog_ptr->enable_all_stepds = false;
}

/*
 * list_find_watch_dog - find an entry in the watch dog list, see common/list.h
 *	for documentation
 * IN key - watch_dog name
 * RET 1 if matches key, 0 otherwise
 */
int list_find_watch_dog(void *x, void *key)
{
	watch_dog_record_t *watch_dog_ptr = (watch_dog_record_t *) x;
	char *watch_dog_name = (char *)key;

	return (!xstrcmp(watch_dog_ptr->watch_dog, watch_dog_name));
}

/*
 * create_watch_dog_record - create a partition record
 * RET a pointer to the record or NULL if error
 */
watch_dog_record_t *create_watch_dog_record(const char *name)
{
	watch_dog_record_t *watch_dog_ptr = xmalloc(sizeof(*watch_dog_ptr));

	last_part_update = time(NULL);

	_init_watch_dog_record(watch_dog_ptr);
	watch_dog_ptr->watch_dog = xstrdup(name);

	(void) list_append(watch_dog_list, watch_dog_ptr);

	return watch_dog_ptr;

}

/*
 * _build_single_watchdogline_info - get a array of watch_dog_record_t
 *	structures from the slurm.conf reader, build table, and set values
 * RET 0 if no error, error code otherwise
 * Note: Operates on common variables
 * global: xxxx - global partition list pointer
 *	xxxx - default parameters for a partition
 */	
static int _build_single_watchdogline_info(watch_dog_record_t *watch)
{
	watch_dog_record_t *watch_dog_ptr = NULL;

	if (list_find_first(watch_dog_list, &list_find_watch_dog, watch->watch_dog)) {
		error("%s: %s specified more than once, latest value used",
		      __func__,  watch->watch_dog);
		list_delete_first(watch_dog_list,  &list_find_watch_dog, watch->watch_dog);
	}

	watch_dog_ptr = create_watch_dog_record(watch->watch_dog);  
	// if(watch->watch_dog)
	// 	watch_dog_ptr->watch_dog = xstrdup(watch->watch_dog);

	if(watch->script)
		watch_dog_ptr->script = xstrdup(watch->script);

	if(watch->account)
		watch_dog_ptr->account = xstrdup(watch->account);

	if(watch->describe)
		watch_dog_ptr->describe = xstrdup(watch->describe);

   	watch_dog_ptr->init_time  = watch->init_time;
   	watch_dog_ptr->period     = watch->period;
	watch_dog_ptr->show_flags = watch->show_flags;
	watch_dog_ptr->enable_all_nodes = watch->enable_all_nodes;
	watch_dog_ptr->enable_all_stepds = watch->enable_all_stepds;
	if(!(watch_dog_ptr->watch_dog) || !(watch_dog_ptr->script))
		error("watch dog  has invalid options, "
			      "please check your configuration");

    return 0;
}
#endif

/*
 * _build_single_partitionline_info - get a array of slurm_conf_partition_t
 *	structures from the slurm.conf reader, build table, and set values
 * RET 0 if no error, error code otherwise
 * Note: Operates on common variables
 * global: part_list - global partition list pointer
 *	default_part - default parameters for a partition
 */
static int _build_single_partitionline_info(slurm_conf_partition_t *part)
{
	part_record_t *part_ptr;

	if (list_find_first(part_list, &list_find_part, part->name))
		fatal("%s: duplicate entry for partition %s",
		      __func__, part->name);

	part_ptr = create_ctld_part_record(part->name);

	if (part->default_flag) {
		if (default_part_name &&
		    xstrcmp(default_part_name, part->name)) {
			info("_parse_part_spec: changing default partition "
			     "from %s to %s", default_part_name, part->name);
			default_part_loc->flags &= (~PART_FLAG_DEFAULT);
		}
		xfree(default_part_name);
		default_part_name = xstrdup(part->name);
		default_part_loc = part_ptr;
		part_ptr->flags |= PART_FLAG_DEFAULT;
	}

	part_ptr->cpu_bind = part->cpu_bind;

	if (part->preempt_mode != NO_VAL16)
		part_ptr->preempt_mode = part->preempt_mode;

	if (part->disable_root_jobs == NO_VAL8) {
		if (slurm_conf.conf_flags & CONF_FLAG_DRJ)
			part_ptr->flags |= PART_FLAG_NO_ROOT;
	} else if (part->disable_root_jobs) {
		part_ptr->flags |= PART_FLAG_NO_ROOT;
	} else {
		part_ptr->flags &= (~PART_FLAG_NO_ROOT);
	}
	if (part_ptr->flags & PART_FLAG_NO_ROOT)
		debug2("partition %s does not allow root jobs", part_ptr->name);

	if ((part->default_time != NO_VAL) &&
	    (part->default_time > part->max_time)) {
		info("partition %s DefaultTime exceeds MaxTime (%u > %u)",
		     part->name, part->default_time, part->max_time);
		part->default_time = NO_VAL;
	}

	if (part->exclusive_user)
		part_ptr->flags |= PART_FLAG_EXCLUSIVE_USER;
	if (part->exclusive_topo)
		part_ptr->flags |= PART_FLAG_EXCLUSIVE_TOPO;
	if (part->hidden_flag)
		part_ptr->flags |= PART_FLAG_HIDDEN;
	if (part->power_down_on_idle)
		part_ptr->flags |= PART_FLAG_PDOI;
	if (part->root_only_flag)
		part_ptr->flags |= PART_FLAG_ROOT_ONLY;
	if (part->req_resv_flag)
		part_ptr->flags |= PART_FLAG_REQ_RESV;
	if (part->lln_flag)
		part_ptr->flags |= PART_FLAG_LLN;
#ifdef __METASTACK_NEW_HETPART_SUPPORT
	if (part->hetpart_flag)
		part_ptr->meta_flags |= PART_METAFLAG_HETPART;
#endif
#ifdef __METASTACK_NEW_PART_RBN
	if (part->rbn_flag)
		part_ptr->meta_flags |= PART_METAFLAG_RBN;
#endif	
#ifdef __METASTACK_NEW_PART_LLS
	if (part->lls_flag)
		part_ptr->meta_flags |= PART_METAFLAG_LLS;
#endif
	part_ptr->max_time       = part->max_time;
	part_ptr->def_mem_per_cpu = part->def_mem_per_cpu;
	part_ptr->default_time   = part->default_time;
	FREE_NULL_LIST(part_ptr->job_defaults_list);
	part_ptr->job_defaults_list =
		job_defaults_copy(part->job_defaults_list);
	part_ptr->max_cpus_per_node = part->max_cpus_per_node;
	part_ptr->max_cpus_per_socket = part->max_cpus_per_socket;
	part_ptr->max_share      = part->max_share;
	part_ptr->max_mem_per_cpu = part->max_mem_per_cpu;
	part_ptr->max_nodes      = part->max_nodes;
	part_ptr->max_nodes_orig = part->max_nodes;
	part_ptr->min_nodes      = part->min_nodes;
	part_ptr->min_nodes_orig = part->min_nodes;
	part_ptr->over_time_limit = part->over_time_limit;
	part_ptr->preempt_mode   = part->preempt_mode;
	part_ptr->priority_job_factor = part->priority_job_factor;
#ifdef __METASTACK_PART_PRIORITY_WEIGHT
	if (!part_ptr->priority_params) {
		_init_part_record_priority_params(part_ptr);
	}
	part_ptr->priority_params->priority_favor_small  = part->priority_favor_small;
	part_ptr->priority_params->priority_weight_age   = part->priority_weight_age;
	part_ptr->priority_params->priority_weight_assoc = part->priority_weight_assoc;
	part_ptr->priority_params->priority_weight_fs    = part->priority_weight_fs;
	part_ptr->priority_params->priority_weight_js    = part->priority_weight_js;
	part_ptr->priority_params->priority_weight_part  = part->priority_weight_part;
	part_ptr->priority_params->priority_weight_qos   = part->priority_weight_qos;
	part_ptr->priority_params->priority_weight_tres  = xstrdup(part->priority_weight_tres);
	check_partition_prio_weights(part_ptr);
#endif
	part_ptr->priority_tier  = part->priority_tier;
	part_ptr->resume_timeout = part->resume_timeout;
	part_ptr->state_up       = part->state_up;
	part_ptr->suspend_time   = part->suspend_time;
	part_ptr->suspend_timeout = part->suspend_timeout;
	part_ptr->grace_time     = part->grace_time;
	part_ptr->cr_type        = part->cr_type;
#ifdef __METASTACK_NEW_SUSPEND_KEEP_IDLE
    part_ptr->suspend_idle = part->suspend_idle;
#endif

	part_ptr->allow_alloc_nodes = xstrdup(part->allow_alloc_nodes);
	part_ptr->allow_groups = xstrdup(part->allow_groups);
	part_ptr->alternate = xstrdup(part->alternate);
	part_ptr->nodes = xstrdup(part->nodes);
	part_ptr->orig_nodes = xstrdup(part->nodes);
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
	part_ptr->standby_nodes->parameters = xstrdup(part->standby_node_parameters);
	part_ptr->standby_nodes->nodes = xstrdup(part->standby_nodes);
#endif

#ifdef __METASTACK_PART_PRIORITY_WEIGHT
	if (partition_has_prio_weight(part_ptr, PRIO_TRES)) {
		set_partition_tres_weights(part_ptr->priority_params->priority_weight_tres,
						part_ptr, false);
	}
#endif
	
	if (part->billing_weights_str) {
		set_partition_billing_weights(part->billing_weights_str,
					      part_ptr, true);
	}

	if (part->allow_accounts) {
		part_ptr->allow_accounts = xstrdup(part->allow_accounts);
		part_ptr->allow_accts_list =
			accounts_list_build(part_ptr->allow_accounts, false);
	}

	if (part->allow_qos) {
		part_ptr->allow_qos = xstrdup(part->allow_qos);
		qos_list_build(part_ptr->allow_qos,&part_ptr->allow_qos_bitstr);
	}

	if (part->deny_accounts) {
		part_ptr->deny_accounts = xstrdup(part->deny_accounts);
		part_ptr->deny_accts_list =
			accounts_list_build(part_ptr->deny_accounts, false);
	}

	if (part->deny_qos) {
		part_ptr->deny_qos = xstrdup(part->deny_qos);
		qos_list_build(part_ptr->deny_qos, &part_ptr->deny_qos_bitstr);
	}

	if (part->qos_char) {
		slurmdb_qos_rec_t qos_rec;
		part_ptr->qos_char = xstrdup(part->qos_char);

		memset(&qos_rec, 0, sizeof(slurmdb_qos_rec_t));
		qos_rec.name = part_ptr->qos_char;
		if (assoc_mgr_fill_in_qos(
			    acct_db_conn, &qos_rec, accounting_enforce,
			    (slurmdb_qos_rec_t **)&part_ptr->qos_ptr, 0)
		    != SLURM_SUCCESS) {
			fatal("Partition %s has an invalid qos (%s), "
			      "please check your configuration",
			      part_ptr->name, qos_rec.name);
		}
		if (part_ptr->qos_ptr) {
			if ((part_ptr->qos_ptr->flags & QOS_FLAG_PART_QOS) &&
			    (part_ptr->qos_ptr->flags & QOS_FLAG_RELATIVE))
				fatal("QOS %s is a relative QOS. A relative QOS must be unique per partition. Please check your configuration and adjust accordingly",
				      part_ptr->qos_ptr->name);
			part_ptr->qos_ptr->flags |= QOS_FLAG_PART_QOS;
		}
	}

	return 0;
}

#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
static int _build_all_watchdog_info(void)
{
	watch_dog_record_t **watr_array = NULL;
	int count = 0;
	int i;	
	count = slurm_conf_watch_dog_array(&watr_array);
	if (count == 0) {
		debug("No watch dog information available!");
		return SLURM_ERROR;
	}

	for (i = 0; i < count; i++)
		_build_single_watchdogline_info(watr_array[i]);

	return SLURM_SUCCESS;
}
#endif

/*
 * _build_all_partitionline_info - get a array of slurm_conf_partition_t
 *	structures from the slurm.conf reader, build table, and set values
 * Note: Operates on common variables
 * global: part_list - global partition list pointer
 *	default_part - default parameters for a partition
 */
static void _build_all_partitionline_info(void)
{
	slurm_conf_partition_t **ptr_array;
	int count;
	int i;

	count = slurm_conf_partition_array(&ptr_array);

	for (i = 0; i < count; i++)
		_build_single_partitionline_info(ptr_array[i]);
}

static int _set_max_part_prio(void *x, void *arg)
{
	part_record_t *part_ptr = x;

	if (part_ptr->priority_job_factor > part_max_priority)
		part_max_priority = part_ptr->priority_job_factor;

	return 0;
}

static int _reset_part_prio(void *x, void *arg)
{
	part_record_t *part_ptr = x;

	/* protect against div0 if all partition priorities are zero */
	if (part_max_priority == 0) {
		part_ptr->norm_priority = 0;
		return 0;
	}

	part_ptr->norm_priority = (double)part_ptr->priority_job_factor /
				  (double)part_max_priority;

	return 0;
}

/* _sync_part_prio - Set normalized partition priorities */
static void _sync_part_prio(void)
{
	/* reset global value from part list */
	part_max_priority = DEF_PART_MAX_PRIORITY;
	list_for_each(part_list, _set_max_part_prio, NULL);
	/* renormalize values after finding new max */
	list_for_each(part_list, _reset_part_prio, NULL);
}

static int _foreach_requeue_job_node_failed(void *x, void *arg)
{
	job_record_t *job_ptr = x;
	job_record_t *het_job_leader;
	int rc = SLURM_SUCCESS;

	xassert(job_ptr->magic == JOB_MAGIC);

	if (!IS_JOB_NODE_FAILED(job_ptr) && !IS_JOB_REQUEUED(job_ptr))
		return SLURM_SUCCESS;

	het_job_leader = find_job_record(job_ptr->het_job_id);
	if (het_job_leader && het_job_leader->batch_flag &&
	    het_job_leader->details &&
	    het_job_leader->details->requeue &&
	    het_job_leader->part_ptr) {
		info("Requeue het job leader %pJ due to node failure on %pJ",
		     het_job_leader, job_ptr);
		if ((rc = job_requeue(0, het_job_leader->job_id, NULL, false,
				      0)))
			error("Unable to requeue %pJ: %s",
			      het_job_leader, slurm_strerror(rc));
	} else if (job_ptr->batch_flag && job_ptr->details &&
		   job_ptr->details->requeue && job_ptr->part_ptr) {
		info("Requeue job %pJ due to node failure",
		     job_ptr);
		if ((rc = job_requeue(0, job_ptr->job_id, NULL, false, 0)))
			error("Unable to requeue %pJ: %s",
			      job_ptr, slurm_strerror(rc));
	}
	job_state_unset_flag(job_ptr, JOB_REQUEUE);

#ifdef __METASTACK_OPT_CACHE_QUERY
	_add_job_state_to_queue(job_ptr);
#endif
	return rc;
}

static void _requeue_job_node_failed(void)
{
	xassert(job_list);

	(void) list_for_each_nobreak(job_list,
				     _foreach_requeue_job_node_failed, NULL);
}

static void _abort_job(job_record_t *job_ptr, uint32_t job_state,
		       uint16_t state_reason, char *reason_string)
{
	time_t now = time(NULL);

	job_state_set(job_ptr, (job_state | JOB_COMPLETING));
	build_cg_bitmap(job_ptr);
	job_ptr->end_time = MIN(job_ptr->end_time, now);
	job_ptr->state_reason = state_reason;
	xfree(job_ptr->state_desc);
	job_ptr->state_desc = xstrdup(reason_string);
	job_completion_logger(job_ptr, false);
	if (job_ptr->job_state == JOB_NODE_FAIL) {
		/* build_cg_bitmap() may clear JOB_COMPLETING */
		epilog_slurmctld(job_ptr);
	}
#ifdef __METASTACK_OPT_CACHE_QUERY
	_add_job_state_to_queue(job_ptr);
#endif
}

static int _mark_het_job_unused(void *x, void *arg)
{
	job_record_t *job_ptr = x;
	job_ptr->bit_flags &= (~HET_JOB_FLAG);
	return 0;
}

static int _mark_het_job_used(void *x, void *arg)
{
	job_record_t *job_ptr = x;
	job_ptr->bit_flags |= HET_JOB_FLAG;
	return 0;
}

static int _test_het_job_used(void *x, void *arg)
{
	job_record_t *job_ptr = x;

	if ((job_ptr->het_job_id == 0) || IS_JOB_FINISHED(job_ptr))
		return 0;
	if (job_ptr->bit_flags & HET_JOB_FLAG)
		return 0;

	error("Incomplete hetjob being aborted %pJ", job_ptr);
	_abort_job(job_ptr, JOB_FAILED, FAIL_SYSTEM, "incomplete hetjob");

	return 0;
}

/*
 * Validate heterogeneous jobs
 *
 * Make sure that every active (not yet complete) job has all of its components
 * and they are all in the same state. Also rebuild het_job_list.
 * If hetjob is corrupted, aborts and removes it from job_list.
 */
static void _validate_het_jobs(void)
{
	list_itr_t *job_iterator;
	job_record_t *job_ptr, *het_job_ptr;
	hostset_t *hs;
	char *job_id_str;
	uint32_t job_id;
	bool het_job_valid;

	list_for_each(job_list, _mark_het_job_unused, NULL);

	job_iterator = list_iterator_create(job_list);
	while ((job_ptr = list_next(job_iterator))) {
		/* Checking for corrupted hetjob components */
		if (job_ptr->het_job_offset != 0) {
			het_job_ptr = find_job_record(job_ptr->het_job_id);
			if (!het_job_ptr) {
				error("Could not find hetjob leader (JobId=%u) of %pJ. Aborting and removing job as it is corrupted.",
				      job_ptr->het_job_id, job_ptr);
				_abort_job(job_ptr, JOB_FAILED, FAIL_SYSTEM,
					   "invalid het_job_id_set");
				if (list_delete_item(job_iterator) != 1)
					error("Not able to remove the job.");
				continue;
			}
			if (job_ptr->het_job_id &&
			    (job_ptr->job_id == job_ptr->het_job_id)) {
				error("Invalid HetJob component %pJ HetJobIdSet=%s. Aborting and removing job.",
				      job_ptr,
				      job_ptr->het_job_id_set);
				_abort_job(job_ptr, JOB_FAILED, FAIL_SYSTEM,
					   "Invalid HetJob component");
				if (list_delete_item(job_iterator) != 1)
					error("Not able to remove the job.");
				continue;
			}
		}

		if ((job_ptr->het_job_id == 0) ||
		    (job_ptr->het_job_offset != 0))
			continue;
		/* active het job leader found */
		FREE_NULL_LIST(job_ptr->het_job_list);
		job_id_str = NULL;
		/* Need to wrap numbers with brackets for hostset functions */
		xstrfmtcat(job_id_str, "[%s]", job_ptr->het_job_id_set);
		hs = hostset_create(job_id_str);
		xfree(job_id_str);
		if (!hs) {
			error("%pJ has invalid het_job_id_set(%s). Aborting and removing job as it is corrupted.",
			      job_ptr, job_ptr->het_job_id_set);
			_abort_job(job_ptr, JOB_FAILED, FAIL_SYSTEM,
				   "invalid het_job_id_set");
			if (list_delete_item(job_iterator) != 1)
				error("Not able to remove the job.");
			continue;
		}
		job_ptr->het_job_list = list_create(NULL);
		het_job_valid = true;	/* assume valid for now */
		while (het_job_valid && (job_id_str = hostset_shift(hs))) {
			job_id = (uint32_t) strtoll(job_id_str, NULL, 10);
			het_job_ptr = find_job_record(job_id);
			if (!het_job_ptr) {
				error("Could not find JobId=%u, part of hetjob JobId=%u",
				      job_id, job_ptr->job_id);
				het_job_valid = false;
			} else if (het_job_ptr->het_job_id !=
				   job_ptr->job_id) {
				error("Invalid state of JobId=%u, part of hetjob JobId=%u",
				      job_id, job_ptr->job_id);
				het_job_valid = false;
			} else {
				list_append(job_ptr->het_job_list,
					    het_job_ptr);
			}
			free(job_id_str);
		}
		hostset_destroy(hs);
		if (het_job_valid) {
			list_for_each(job_ptr->het_job_list, _mark_het_job_used,
				      NULL);
		}
	}
	list_iterator_destroy(job_iterator);

	list_for_each(job_list, _test_het_job_used, NULL);
}

/* Log an error if SlurmdUser is not root and any cgroup plugin is used */
static void _test_cgroup_plugin_use(void)
{
	if (xstrstr(slurm_conf.task_plugin, "cgroup"))
		error("task/cgroup plugin will not work unless SlurmdUser is root");

	if (xstrstr(slurm_conf.proctrack_type, "cgroup"))
		error("proctrack/cgroup plugin will not work unless SlurmdUser is root");
}


static void _sync_steps_to_conf(job_record_t *job_ptr)
{
	list_itr_t *step_iterator;
	step_record_t *step_ptr;

	step_iterator = list_iterator_create (job_ptr->step_list);
	while ((step_ptr = list_next(step_iterator))) {
		if (step_ptr->state < JOB_RUNNING)
			continue;
		FREE_NULL_BITMAP(step_ptr->step_node_bitmap);
		if (step_ptr->step_layout &&
		    step_ptr->step_layout->node_list &&
		    (node_name2bitmap(step_ptr->step_layout->node_list, false,
				      &step_ptr->step_node_bitmap))) {
			error("Invalid step_node_list (%s) for %pS",
			      step_ptr->step_layout->node_list, step_ptr);
			delete_step_record(job_ptr, step_ptr);
		} else if (step_ptr->step_node_bitmap == NULL) {
			error("Missing node_list for %pS", step_ptr);
			delete_step_record(job_ptr, step_ptr);
		}
	}

	list_iterator_destroy (step_iterator);
}

static int _sync_detail_bitmaps(job_record_t *job_ptr)
{
	if (job_ptr->details == NULL)
		return SLURM_SUCCESS;

	FREE_NULL_BITMAP(job_ptr->details->req_node_bitmap);

	if ((job_ptr->details->req_nodes) &&
	    (node_name2bitmap(job_ptr->details->req_nodes, false,
			      &job_ptr->details->req_node_bitmap))) {
		error("Invalid req_nodes (%s) for %pJ",
		      job_ptr->details->req_nodes, job_ptr);
		return SLURM_ERROR;
	}

	FREE_NULL_BITMAP(job_ptr->details->exc_node_bitmap);
	if ((job_ptr->details->exc_nodes) &&
	    (node_name2bitmap(job_ptr->details->exc_nodes, true,
			      &job_ptr->details->exc_node_bitmap))) {
		error("Invalid exc_nodes (%s) for %pJ",
		      job_ptr->details->exc_nodes, job_ptr);
		return SLURM_ERROR;
	}

	/*
	 * If a nodelist has been provided with more nodes than are required
	 * for the job, translate this into an exclusion of all nodes except
	 * those requested.
	 */
	if (job_ptr->details->req_node_bitmap &&
	    (bit_set_count(job_ptr->details->req_node_bitmap) >
	     job_ptr->details->min_nodes)) {
		if (!job_ptr->details->exc_node_bitmap)
			job_ptr->details->exc_node_bitmap =
				bit_alloc(node_record_count);
		bit_or_not(job_ptr->details->exc_node_bitmap,
			   job_ptr->details->req_node_bitmap);
		FREE_NULL_BITMAP(job_ptr->details->req_node_bitmap);
	}

	return SLURM_SUCCESS;
}

/*
 * _sync_jobs_to_conf - Sync current slurm.conf configuration for existing jobs.
 *	This should be called after rebuilding node, part, and gres information,
 *	but before using any job entries.
 * global: last_job_update - time of last job table update
 *	job_list - pointer to global job list
 */
void _sync_jobs_to_conf(void)
{
	list_itr_t *job_iterator;
	job_record_t *job_ptr;
	part_record_t *part_ptr;
	List part_ptr_list = NULL;
	bool job_fail = false;
	time_t now = time(NULL);
	bool gang_flag = false;

	xassert(job_list);

	if (slurm_conf.preempt_mode & PREEMPT_MODE_GANG)
		gang_flag = true;

	job_iterator = list_iterator_create(job_list);
	while ((job_ptr = list_next(job_iterator))) {
		xassert (job_ptr->magic == JOB_MAGIC);
		job_fail = false;

		/*
		 * This resets the req/exc node bitmaps, so even if the job is
		 * finished it still needs to happen just in case the job is
		 * requeued.
		 */
		if (_sync_detail_bitmaps(job_ptr)) {
			job_fail = true;
			if (job_ptr->details) {
				/*
				 * job can't be requeued because either
				 * req_nodes or exc_nodes can't be satisfied.
				 */
				job_ptr->details->requeue = false;
			}
		}

		/*
		 * While the job is completed at this point there is code in
		 * _job_requeue_op() that requires the part_ptr to be set in
		 * order to requeue a job.  We also need to set it to NULL if
		 * the partition was removed or we will be pointing at bad
		 * data.  This is the safest/easiest place to do it.
		 */

		if (job_ptr->partition == NULL) {
			error("No partition for %pJ", job_ptr);
			part_ptr = NULL;
			job_fail = true;
		} else {
			char *err_part = NULL;
			part_ptr = find_part_record(job_ptr->partition);
			if (part_ptr == NULL) {
				part_ptr_list = get_part_list(
					job_ptr->partition,
					&err_part);
				if (part_ptr_list) {
					part_ptr = list_peek(part_ptr_list);
					if (list_count(part_ptr_list) == 1)
						FREE_NULL_LIST(part_ptr_list);
				}
			}
			if (part_ptr == NULL) {
				error("Invalid partition (%s) for %pJ",
				      err_part, job_ptr);
				xfree(err_part);
				job_fail = true;
			}
		}
		job_ptr->part_ptr = part_ptr;
		FREE_NULL_LIST(job_ptr->part_ptr_list);
		if (part_ptr_list) {
			job_ptr->part_ptr_list = part_ptr_list;
			part_ptr_list = NULL;	/* clear for next job */
		}

		/*
		 * If the job is finished there is no reason to do anything
		 * below this.
		 */
		if (IS_JOB_COMPLETED(job_ptr))
			continue;

		FREE_NULL_BITMAP(job_ptr->node_bitmap_cg);
		if (job_ptr->nodes_completing &&
		    node_name2bitmap(job_ptr->nodes_completing,
				     false,  &job_ptr->node_bitmap_cg)) {
			error("Invalid nodes (%s) for %pJ",
			      job_ptr->nodes_completing, job_ptr);
			job_fail = true;
		}
		FREE_NULL_BITMAP(job_ptr->node_bitmap);
		if (job_ptr->nodes &&
		    node_name2bitmap(job_ptr->nodes, false,
				     &job_ptr->node_bitmap) && !job_fail) {
			error("Invalid nodes (%s) for %pJ",
			      job_ptr->nodes, job_ptr);
			job_fail = true;
		}
		FREE_NULL_BITMAP(job_ptr->node_bitmap_pr);
#ifndef HAVE_FRONT_END
		if (job_ptr->nodes_pr &&
		    node_name2bitmap(job_ptr->nodes_pr,
				     false,  &job_ptr->node_bitmap_pr)) {
			error("Invalid nodes (%s) for %pJ",
			      job_ptr->nodes_pr, job_ptr);
			job_fail = true;
		}
#endif
		if (reset_node_bitmap(job_ptr))
			job_fail = true;
		if (!job_fail &&
		    job_ptr->job_resrcs &&
		    (slurm_select_cr_type() || gang_flag) &&
		    valid_job_resources(job_ptr->job_resrcs)) {
			error("Aborting %pJ due to change in socket/core configuration of allocated nodes",
			      job_ptr);
			job_fail = true;
		}
		if (!job_fail &&
		    gres_job_revalidate(job_ptr->gres_list_req)) {
			error("Aborting %pJ due to use of unsupported GRES options",
			      job_ptr);
			job_fail = true;
			if (job_ptr->details) {
				/* don't attempt to requeue job */
				job_ptr->details->requeue = false;
			}
		}

		if (!job_fail && job_ptr->job_resrcs &&
		    (IS_JOB_RUNNING(job_ptr) || IS_JOB_SUSPENDED(job_ptr)) &&
		    gres_job_revalidate2(job_ptr->job_id,
					 job_ptr->gres_list_alloc,
					 job_ptr->job_resrcs->node_bitmap)) {
			/*
			 * This can be due to the job being allocated GRES
			 * which no longer exist (i.e. the GRES count on some
			 * allocated node changed since when the job started).
			 */
			error("Aborting %pJ due to use of invalid GRES configuration",
			      job_ptr);
			job_fail = true;
		}

		_sync_steps_to_conf(job_ptr);

		build_node_details(job_ptr, false); /* set node_addr */

		if (job_fail) {
			bool was_running = false;
			if (IS_JOB_PENDING(job_ptr)) {
				job_ptr->start_time =
					job_ptr->end_time = time(NULL);
				job_state_set(job_ptr, JOB_NODE_FAIL);
			} else if (IS_JOB_RUNNING(job_ptr)) {
				job_ptr->end_time = time(NULL);
				job_state_set(job_ptr, (JOB_NODE_FAIL |
							JOB_COMPLETING));
				build_cg_bitmap(job_ptr);
				was_running = true;
			} else if (IS_JOB_SUSPENDED(job_ptr)) {
#ifdef __METASTACK_BUG_FIX_SUSPEND_TIME
				job_ptr->tot_sus_time += difftime(now, job_ptr->suspend_time);
				job_ptr->end_time = job_ptr->suspend_time = now;
#else
				job_ptr->end_time = job_ptr->suspend_time;
#endif
				job_state_set(job_ptr, (JOB_NODE_FAIL |
							JOB_COMPLETING));
				build_cg_bitmap(job_ptr);
				job_ptr->tot_sus_time +=
					difftime(now, job_ptr->suspend_time);
				jobacct_storage_g_job_suspend(acct_db_conn,
							      job_ptr);
				was_running = true;
			}
			job_ptr->state_reason = FAIL_DOWN_NODE;
			xfree(job_ptr->state_desc);
			job_completion_logger(job_ptr, false);
			if (job_ptr->job_state == JOB_NODE_FAIL) {
				/* build_cg_bitmap() may clear JOB_COMPLETING */
				epilog_slurmctld(job_ptr);
			}
			if (was_running && job_ptr->batch_flag &&
			    job_ptr->details && job_ptr->details->requeue &&
			    job_ptr->part_ptr) {
				/*
				 * Mark for requeue
				 * see _requeue_job_node_failed()
				 */
				info("Attempting to requeue failed job %pJ",
				     job_ptr);
				job_state_set_flag(job_ptr, JOB_REQUEUE);

				/* Reset node_cnt to exclude vanished nodes */
				job_ptr->node_cnt = bit_set_count(
					job_ptr->node_bitmap_cg);
			}
#ifdef __METASTACK_OPT_CACHE_QUERY
			_add_job_state_to_queue(job_ptr);
#endif
		}
	}

	list_iterator_reset(job_iterator);
	/* This will reinitialize the select plugin database, which
	 * we can only do after ALL job's states and bitmaps are set
	 * (i.e. it needs to be in this second loop) */
	while ((job_ptr = list_next(job_iterator))) {
		if (select_g_select_nodeinfo_set(job_ptr) != SLURM_SUCCESS) {
			error("select_g_select_nodeinfo_set(%pJ): %m",
			      job_ptr);
		}
	}
	list_iterator_destroy(job_iterator);

	last_job_update = now;
}

/*
 * read_slurm_conf - load the slurm configuration from the configured file.
 * read_slurm_conf can be called more than once if so desired.
 * IN recover - replace job, node and/or partition data with latest
 *              available information depending upon value
 *              0 = use no saved state information, rebuild everything from
 *		    slurm.conf contents
 *              1 = recover saved job and trigger state,
 *                  node DOWN/DRAIN/FAIL state and reason information
 *              2 = recover all saved state
 * RET SLURM_SUCCESS if no error, otherwise an error code
 * Note: Operates on common variables only
 */
extern int read_slurm_conf(int recover)
{
	DEF_TIMERS;
	int error_code = SLURM_SUCCESS;
	int rc = 0, load_job_ret = SLURM_SUCCESS;
#ifdef __METASTACK_OPT_CACHE_QUERY
	uint16_t old_cache_query = slurm_conf.cache_query;
	uint32_t old_query_port = slurm_conf.query_port;
	uint16_t old_cachedup_realtime = cachedup_realtime;
#endif
	char *old_auth_type = xstrdup(slurm_conf.authtype);
	char *old_bb_type = xstrdup(slurm_conf.bb_type);
	char *old_cred_type = xstrdup(slurm_conf.cred_type);
	char *old_job_container_type = xstrdup(slurm_conf.job_container_plugin);
	char *old_preempt_type = xstrdup(slurm_conf.preempt_type);
	char *old_sched_type = xstrdup(slurm_conf.schedtype);
	char *old_select_type = xstrdup(slurm_conf.select_type);
	char *old_switch_type = xstrdup(slurm_conf.switch_type);
	char *state_save_dir = xstrdup(slurm_conf.state_save_location);
	char *tmp_ptr = NULL;
	uint16_t old_select_type_p = slurm_conf.select_type_param;
	bool cgroup_mem_confinement = false;
	uint16_t reconfig_flags = slurm_conf.reconfig_flags;
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
	bool rebuild = false;
	bool build_resource = false;
#endif

	/* initialization */
	START_TIMER;

	_init_all_slurm_conf();
#ifdef __METASTACK_OPT_CACHE_QUERY
	if((old_cache_query == 0 &&  slurm_conf.cache_query != 0) || (old_cache_query != 0 &&  slurm_conf.cache_query == 
	0) || slurm_conf.query_port != old_query_port){
		slurm_conf.cache_query = old_cache_query;
		slurm_conf.query_port = old_query_port;
		cachedup_realtime = old_cachedup_realtime;
		error("Cache query configuration modification failed.");
	}
#endif
	cgroup_conf_init();

	cgroup_mem_confinement = cgroup_memcg_job_confinement();

#ifdef __METASTACK_OPT_MSG_OUTPUT
	if (xstrcasestr(slurm_conf.slurmctld_params, "enable_reason_detail"))
		enable_reason_detail = true;
#endif

#ifdef __METASTACK_OPT_HIGH_THROUGHPUT_SRUN_JOB_COM
	if (xstrcasestr(slurm_conf.slurmctld_params, "ignore_srun_job_complete")) {
		ignore_srun_job_complete = true;
	} else {
		ignore_srun_job_complete = false;
	}
	debug("ignore_srun_job_complete is %s", ignore_srun_job_complete ? "enabled" : "disabled");
#endif

	if (slurm_conf.job_acct_oom_kill && cgroup_mem_confinement)
		fatal("Jobs memory is being constrained by both TaskPlugin cgroup and JobAcctGather plugin. This enables two incompatible memory enforcement mechanisms, one of them must be disabled.");
	else if (slurm_conf.job_acct_oom_kill)
		info("Memory enforcing by using JobAcctGather's mechanism is discouraged, task/cgroup is recommended where available.");
	else if (!cgroup_mem_confinement)
		info("No memory enforcing mechanism configured.");

	if (slurm_conf.slurmd_user_id != 0)
		_test_cgroup_plugin_use();

	if (topology_g_init() != SLURM_SUCCESS)
		fatal("Failed to initialize topology plugin");

	if (xstrcasestr(slurm_conf.slurmctld_params, "enable_stepmgr") &&
	    !(slurm_conf.prolog_flags & PROLOG_FLAG_CONTAIN))
		fatal("STEP_MGR not supported without PrologFlags=contain");

	/* Build node and partition information based upon slurm.conf file */
	build_all_nodeline_info(false, slurmctld_tres_cnt);
	/* Increase node table to handle dynamic nodes. */
	if ((slurm_conf.max_node_cnt != NO_VAL) &&
	    node_record_count < slurm_conf.max_node_cnt) {
		node_record_count = slurm_conf.max_node_cnt;
		grow_node_record_table_ptr();
	} else {
		/* Lock node_record_table_ptr from growing */
		slurm_conf.max_node_cnt = node_record_count;
	}
	if (slurm_conf.max_node_cnt == 0) {
		/*
		 * Set to 1 so bitmaps will be created but don't allow any nodes
		 * to be created.
		 */
		node_record_count = 1;
		grow_node_record_table_ptr();
	}

	(void)acct_storage_g_reconfig(acct_db_conn, 0);
	build_all_frontend_info(false);
	_handle_all_downnodes();
	_build_all_partitionline_info();
#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
	_build_all_watchdog_info();
#endif

#ifdef __METASTACK_OPT_APP  
	_build_all_app_info();  
#endif

	restore_front_end_state(recover);

	/*
	 * Currently load/dump_state_lite has to run before load_all_job_state.
	 * FIXME: this stores a single string, this should probably move into
	 * the job state file as it's only pertinent to job accounting.
	 */
	load_config_state_lite();
	dump_config_state_lite();

	update_logging();
	if (jobcomp_g_init() != SLURM_SUCCESS)
		fatal("Failed to initialize jobcomp plugin");
	if (controller_init_scheduling(
		(slurm_conf.preempt_mode & PREEMPT_MODE_GANG)) != SLURM_SUCCESS) {
		fatal("Failed to initialize the various schedulers");
	}

	if (default_part_loc == NULL)
		error("%s: default partition not set.", __func__);

	if (node_record_count < 1) {
		error("%s: no nodes configured.", __func__);
		error_code = EINVAL;
		goto end_it;
	}

	/*
	 * Node reordering may be done by the topology plugin.
	 * Reordering the table must be done before hashing the
	 * nodes, and before any position-relative bitmaps are created.
	 *
	 * Sort the nodes read in from the slurm.conf first before restoring
	 * the dynamic nodes from the state file to prevent dynamic nodes from
	 * being sorted -- which can cause problems with heterogenous jobs and
	 * the order of the sockets changing on startup.
	 */
	_sort_node_record_table_ptr();

	/*
	 * Load node state which includes dynamic nodes so that dynamic nodes
	 * can be included in topology.
	 */
	if (recover == 0) {		/* Build everything from slurm.conf */
		_set_features(node_record_table_ptr, node_record_count,
			      recover);
	} else if (recover == 1) {	/* Load job & node state files */
		(void) load_all_node_state(true);
		_set_features(node_record_table_ptr, node_record_count,
			      recover);
		(void) load_all_front_end_state(true);
	} else if (recover > 1) {	/* Load node, part & job state files */
		(void) load_all_node_state(false);
		_set_features(NULL, 0, recover);
		(void) load_all_front_end_state(false);
	}

	rehash_node();
	topology_g_build_config();

	rehash_jobs();
	_validate_slurmd_addr();

	_stat_slurm_dirs();

#ifdef __METASTACK_BUG_PROCESS_DISTRIBUTION
	init_disable_change_proc_dist();
#endif

#ifdef __METASTACK_BUG_OVERLAP_NODE_DIST
	_init_enable_overlap_node_lb();
#endif
	_init_bitmaps();

	/*
	 * Set standard features and preserve the plugin controlled ones.
	 */
	if (recover == 0) {		/* Build everything from slurm.conf */
		load_last_job_id();
		reset_first_job_id();
		controller_reconfig_scheduling();
	} else if (recover == 1) {	/* Load job & node state files */
		load_job_ret = load_all_job_state();
	} else if (recover > 1) {	/* Load node, part & job state files */
		reconfig_flags |= RECONFIG_KEEP_PART_INFO;
#ifdef __METASTACK_OPT_APP
		reconfig_flags |= RECONFIG_KEEP_APP_INFO;
#endif
		load_job_ret = load_all_job_state();
	}
	(void) load_all_part_state(reconfig_flags);
#ifdef __METASTACK_OPT_APP
	(void) load_all_app_state(reconfig_flags);
#endif
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
	(void) load_all_part_borrow_nodes(&rebuild);
	valid_node_borrow_interval();
#endif

	/*
	 * _build_node_config_bitmaps() must be called before
	 * build_features_list_*() and before restore_node_features()
	 */
	_build_node_config_bitmaps();
	/* _gres_reconfig needs to happen before restore_node_features */
	_gres_reconfig();
	/* NOTE: Run restore_node_features before _restore_job_accounting */
	restore_node_features(recover);

	if ((node_features_g_count() > 0) &&
	    (node_features_g_get_node(NULL) != SLURM_SUCCESS))
		error("failed to initialize node features");

	/*
	 * _build_bitmaps() must follow node_features_g_get_node() and
	 * precede build_features_list_*()
	 */
	_build_bitmaps();

	/* Active and available features can be different on -R */
	if ((node_features_g_count() == 0) && (recover != 2))
		build_feature_list_eq();
	else
		build_feature_list_ne();

	_sync_part_prio();
	_build_part_bitmaps(); /* Must be called after build_feature_list_*() */

#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
	rebuild |= _restore_all_partition_nodes_borrowed_info();
	if (rebuild) {
		clear_all_partitions_pending_jobs(part_list);
		get_all_partitions_pending_jobs(job_list);
		build_resource = validate_all_partitions_borrow_nodes(false);
	}
#endif

	if ((select_g_node_init() != SLURM_SUCCESS) ||
	    (select_g_state_restore(state_save_dir) != SLURM_SUCCESS) ||
	    (select_g_job_init(job_list) != SLURM_SUCCESS))
		fatal("Failed to initialize node selection plugin state, Clean start required.");

	/*
	 * config_power_mgr() Must be after node and partitions have been loaded
	 * and before any calls to power_save_test().
	 */
	config_power_mgr();

	_sync_jobs_to_conf();		/* must follow select_g_job_init() */

	/*
	 * The burst buffer plugin must be initialized and state loaded before
	 * _sync_nodes_to_jobs(), which calls bb_g_job_init().
	 */
	rc = bb_g_load_state(true);
	error_code = MAX(error_code, rc);	/* not fatal */

	(void) _sync_nodes_to_jobs();
	(void) sync_job_files();

	reserve_port_config(slurm_conf.mpi_params, job_list);

	if (license_update(slurm_conf.licenses) != SLURM_SUCCESS)
		fatal("Invalid Licenses value: %s", slurm_conf.licenses);

	init_requeue_policy();
	init_depend_policy();

	/*
	 * Must be at after nodes and partitions (e.g.
	 * _build_part_bitmaps()) have been created and before
	 * _sync_nodes_to_comp_job().
	 */
	set_cluster_tres(false);

	_validate_het_jobs();
	(void) _sync_nodes_to_comp_job();/* must follow select_g_node_init() */
	_requeue_job_node_failed();
	load_part_uid_allow_list(true);

	/* NOTE: Run load_all_resv_state() before _restore_job_accounting */
	load_all_resv_state(recover);
	if (recover >= 1) {
		trigger_state_restore();
		controller_reconfig_scheduling();
	}

	_restore_job_accounting();

	/* sort config_list by weight for scheduling */
	list_sort(config_list, &list_compare_config);

	/* Update plugins as possible */
	if (xstrcmp(old_auth_type, slurm_conf.authtype)) {
		xfree(slurm_conf.authtype);
		slurm_conf.authtype = old_auth_type;
		old_auth_type = NULL;
		rc =  ESLURM_INVALID_AUTHTYPE_CHANGE;
	}

	if (xstrcmp(old_bb_type, slurm_conf.bb_type)) {
		xfree(slurm_conf.bb_type);
		slurm_conf.bb_type = old_bb_type;
		old_bb_type = NULL;
		rc =  ESLURM_INVALID_BURST_BUFFER_CHANGE;
	}

	if (xstrcmp(old_cred_type, slurm_conf.cred_type)) {
		xfree(slurm_conf.cred_type);
		slurm_conf.cred_type = old_cred_type;
		old_cred_type = NULL;
		rc = ESLURM_INVALID_CRED_TYPE_CHANGE;
	}

	if (xstrcmp(old_job_container_type, slurm_conf.job_container_plugin)) {
		xfree(slurm_conf.job_container_plugin);
		slurm_conf.job_container_plugin = old_job_container_type;
		old_job_container_type = NULL;
		rc =  ESLURM_INVALID_JOB_CONTAINER_CHANGE;
	}

	if (xstrcmp(old_sched_type, slurm_conf.schedtype)) {
		xfree(slurm_conf.schedtype);
		slurm_conf.schedtype = old_sched_type;
		old_sched_type = NULL;
		rc =  ESLURM_INVALID_SCHEDTYPE_CHANGE;
	}

	if (xstrcmp(old_select_type, slurm_conf.select_type)) {
		xfree(slurm_conf.select_type);
		slurm_conf.select_type = old_select_type;
		old_select_type = NULL;
		rc =  ESLURM_INVALID_SELECTTYPE_CHANGE;
	}

	if (xstrcmp(old_switch_type, slurm_conf.switch_type)) {
		xfree(slurm_conf.switch_type);
		slurm_conf.switch_type = old_switch_type;
		old_switch_type = NULL;
		rc = ESLURM_INVALID_SWITCHTYPE_CHANGE;
	}

	if ((slurm_conf.control_cnt < 2) ||
	    (slurm_conf.control_machine[1] == NULL))
		info("%s: backup_controller not specified", __func__);

	error_code = MAX(error_code, rc);	/* not fatal */

	if (xstrcmp(old_preempt_type, slurm_conf.preempt_type)) {
		info("Changing PreemptType from %s to %s",
		     old_preempt_type, slurm_conf.preempt_type);
		(void) preempt_g_fini();
		if (preempt_g_init() != SLURM_SUCCESS)
			fatal("failed to initialize preempt plugin");
	}

	/* Update plugin parameters as possible */
	rc = _preserve_select_type_param(&slurm_conf, old_select_type_p);
	error_code = MAX(error_code, rc);	/* not fatal */

	/*
	 * Restore job accounting info if file missing or corrupted,
	 * an extremely rare situation
	 */
	if (load_job_ret)
		_acct_restore_active_jobs();

	/* Sync select plugin with synchronized job/node/part data */
	gres_reconfig();		/* Clear gres/mps counters */
	select_g_reconfigure();

	_set_response_cluster_rec();

#ifdef __METASTACK_NEW_PART_PARA_SCHED
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
	if (!build_resource)
#endif	
		build_sched_resource();
#endif

	consolidate_config_list(true, true);
	cloud_dns = xstrcasestr(slurm_conf.slurmctld_params, "cloud_dns");
	if ((tmp_ptr = xstrcasestr(slurm_conf.slurmctld_params,
				   "max_powered_nodes="))) {
		max_powered_nodes =
			strtol(tmp_ptr + strlen("max_powered_nodes="),
			       NULL, 10);
	}

	slurm_conf.last_update = time(NULL);
end_it:
	xfree(old_auth_type);
	xfree(old_bb_type);
	xfree(old_cred_type);
	xfree(old_job_container_type);
	xfree(old_preempt_type);
	xfree(old_sched_type);
	xfree(old_select_type);
	xfree(old_switch_type);
	xfree(state_save_dir);

	END_TIMER2(__func__);
	return error_code;

}

/* Add feature to list
 * feature_list IN - destination list, either active_feature_list or
 *	avail_feature_list
 * feature IN - name of the feature to add
 * node_bitmap IN - bitmap of nodes with named feature */
static void _add_config_feature(List feature_list, char *feature,
				bitstr_t *node_bitmap)
{
	node_feature_t *feature_ptr;
	list_itr_t *feature_iter;
	bool match = false;

	/* If feature already in avail_feature_list, just update the bitmap */
	feature_iter = list_iterator_create(feature_list);
	while ((feature_ptr = list_next(feature_iter))) {
		if (xstrcmp(feature, feature_ptr->name))
			continue;
		bit_or(feature_ptr->node_bitmap, node_bitmap);
		match = true;
		break;
	}
	list_iterator_destroy(feature_iter);

	if (!match) {	/* Need to create new avail_feature_list record */
		feature_ptr = xmalloc(sizeof(node_feature_t));
		feature_ptr->magic = FEATURE_MAGIC;
		feature_ptr->name = xstrdup(feature);
		feature_ptr->node_bitmap = bit_copy(node_bitmap);
		list_append(feature_list, feature_ptr);
	}
}

/* Add feature to list
 * feature_list IN - destination list, either active_feature_list or
 *	avail_feature_list
 * feature IN - name of the feature to add
 * node_inx IN - index of the node with named feature */
static void _add_config_feature_inx(List feature_list, char *feature,
				    int node_inx)
{
	node_feature_t *feature_ptr;
	list_itr_t *feature_iter;
	bool match = false;

	/* If feature already in avail_feature_list, just update the bitmap */
	feature_iter = list_iterator_create(feature_list);
	while ((feature_ptr = list_next(feature_iter))) {
		if (xstrcmp(feature, feature_ptr->name))
			continue;
		bit_set(feature_ptr->node_bitmap, node_inx);
		match = true;
		break;
	}
	list_iterator_destroy(feature_iter);

	if (!match) {	/* Need to create new avail_feature_list record */
		feature_ptr = xmalloc(sizeof(node_feature_t));
		feature_ptr->magic = FEATURE_MAGIC;
		feature_ptr->name = xstrdup(feature);
		feature_ptr->node_bitmap = bit_alloc(node_record_count);
		bit_set(feature_ptr->node_bitmap, node_inx);
		list_append(feature_list, feature_ptr);
	}
}

/* _list_delete_feature - delete an entry from the feature list,
 *	see list.h for documentation */
static void _list_delete_feature(void *feature_entry)
{
	node_feature_t *feature_ptr = feature_entry;

	xassert(feature_ptr);
	xassert(feature_ptr->magic == FEATURE_MAGIC);
	xfree (feature_ptr->name);
	FREE_NULL_BITMAP (feature_ptr->node_bitmap);
	xfree (feature_ptr);
}

/*
 * For a configuration where available_features == active_features,
 * build new active and available feature lists
 */
extern void build_feature_list_eq(void)
{
	list_itr_t *config_iterator;
	config_record_t *config_ptr;
	node_feature_t *active_feature_ptr, *avail_feature_ptr;
	list_itr_t *feature_iter;
	char *tmp_str, *token, *last = NULL;

	FREE_NULL_LIST(active_feature_list);
	FREE_NULL_LIST(avail_feature_list);
	active_feature_list = list_create(_list_delete_feature);
	avail_feature_list = list_create(_list_delete_feature);

	config_iterator = list_iterator_create(config_list);
	while ((config_ptr = list_next(config_iterator))) {
		if (config_ptr->feature) {
			tmp_str = xstrdup(config_ptr->feature);
			token = strtok_r(tmp_str, ",", &last);
			while (token) {
				_add_config_feature(avail_feature_list, token,
						    config_ptr->node_bitmap);
				token = strtok_r(NULL, ",", &last);
			}
			xfree(tmp_str);
		}
	}
	list_iterator_destroy(config_iterator);

	/* Copy avail_feature_list to active_feature_list */
	feature_iter = list_iterator_create(avail_feature_list);
	while ((avail_feature_ptr = list_next(feature_iter))) {
		active_feature_ptr = xmalloc(sizeof(node_feature_t));
		active_feature_ptr->magic = FEATURE_MAGIC;
		active_feature_ptr->name = xstrdup(avail_feature_ptr->name);
		active_feature_ptr->node_bitmap =
			bit_copy(avail_feature_ptr->node_bitmap);
		list_append(active_feature_list, active_feature_ptr);
	}
	list_iterator_destroy(feature_iter);
}

/*
 * Log contents of avail_feature_list and active_feature_list
 */
extern void log_feature_lists(void)
{
	node_feature_t *feature_ptr;
	char *node_str;
	list_itr_t *feature_iter;

	feature_iter = list_iterator_create(avail_feature_list);
	while ((feature_ptr = list_next(feature_iter))) {
		node_str = bitmap2node_name(feature_ptr->node_bitmap);
		info("AVAIL FEATURE:%s NODES:%s", feature_ptr->name, node_str);
		xfree(node_str);
	}
	list_iterator_destroy(feature_iter);

	feature_iter = list_iterator_create(active_feature_list);
	while ((feature_ptr = list_next(feature_iter))) {
		node_str = bitmap2node_name(feature_ptr->node_bitmap);
		info("ACTIVE FEATURE:%s NODES:%s", feature_ptr->name, node_str);
		xfree(node_str);
	}
	list_iterator_destroy(feature_iter);
}

/*
 * For a configuration where available_features != active_features,
 * build new active and available feature lists
 */
extern void build_feature_list_ne(void)
{
	node_record_t *node_ptr;
	char *tmp_str, *token, *last = NULL;
	int i;

	FREE_NULL_LIST(active_feature_list);
	FREE_NULL_LIST(avail_feature_list);
	active_feature_list = list_create(_list_delete_feature);
	avail_feature_list = list_create(_list_delete_feature);

	for (i = 0; (node_ptr = next_node(&i)); i++) {
		if (node_ptr->features_act) {
			tmp_str = xstrdup(node_ptr->features_act);
			token = strtok_r(tmp_str, ",", &last);
			while (token) {
				_add_config_feature_inx(active_feature_list,
							token, node_ptr->index);
				token = strtok_r(NULL, ",", &last);
			}
			xfree(tmp_str);
		}
		if (node_ptr->features) {
			tmp_str = xstrdup(node_ptr->features);
			token = strtok_r(tmp_str, ",", &last);
			while (token) {
				_add_config_feature_inx(avail_feature_list,
							token, node_ptr->index);
				token = strtok_r(NULL, ",", &last);
			}
			xfree(tmp_str);
		}
	}
}

/*
 * Update active_feature_list or avail_feature_list
 * feature_list IN - List to update: active_feature_list or avail_feature_list
 * new_features IN - New active_features
 * node_bitmap IN - Nodes with the new active_features value
 */
extern void update_feature_list(List feature_list, char *new_features,
				bitstr_t *node_bitmap)
{
	node_feature_t *feature_ptr;
	list_itr_t *feature_iter;
	char *tmp_str, *token, *last = NULL;

	/*
	 * Clear these nodes from the feature_list record,
	 * then restore as needed
	 */
	feature_iter = list_iterator_create(feature_list);
	while ((feature_ptr = list_next(feature_iter))) {
		bit_and_not(feature_ptr->node_bitmap, node_bitmap);
	}
	list_iterator_destroy(feature_iter);

	if (new_features) {
		tmp_str = xstrdup(new_features);
		token = strtok_r(tmp_str, ",", &last);
		while (token) {
			_add_config_feature(feature_list, token, node_bitmap);
			token = strtok_r(NULL, ",", &last);
		}
		xfree(tmp_str);
	}
	node_features_updated = true;
}

#ifdef __METASTACK_OPT_GRES_CONFIG
/**
 * Removes parentheses and their contents from a given string.
 * str IN - The input string to be modified.
 */
static void remove_parentheses(char *str) {
	if (str == NULL) {
		return; 
	}
	char *src, *dst;
	src = dst = str; 
	while (*src != '\0') { 
		if (*src != '(') { 
			*dst++ = *src; 
			src++; 
		} else { 
			src++; 
			while (*src != '\0' && *src != ')') {
				src++;
			}
			if (*src == ')') { 
				src++; 
			}
		}
	}
	*dst = '\0'; 
}

/**
 * Parses the GRES count from a given GRES name string.
 * 
 * This function takes a GRES name string, removes any parentheses and their contents,
 * and then parses the count of GRES resources. It assumes the GRES name is in the format
 * "type:count" and can contain multiple entries separated by commas.
 * gres_name IN - The GRES name string to parse.
 * return - The total count of GRES resources.
 */
static int parse_gres_count(const char *gres_name) {
	if (gres_name == NULL) {
		error("GRES name is NULL");
		return -1;  
	}
	char *gres_copy = strdup(gres_name);
	if (!gres_copy) {
		error("Memory allocation failed");
		return -1;
	}
	remove_parentheses(gres_copy);
	char *token = strtok(gres_copy, ",");
	int total_count = 0;

	while (token != NULL) {
		char *last_colon = strrchr(token, ':');
		if (last_colon != NULL) {
			char *endptr;
			long num = strtol(last_colon + 1, &endptr, 10);
			if (*endptr == '\0' && endptr > last_colon + 1) {
				total_count += (int)num;
			} else {
				error("Failed to parse GRES number from gres_name");
			}
		} else {
			error("No colon found in gres_name");
		}
		token = strtok(NULL, ",");
	}

	free(gres_copy);
	return total_count;
}
/**
* Cleans up and frees memory allocated within each element of a parsed lines array.
*
* This function iterates through each element of the parsed_lines array and frees
* the memory allocated for the key, value, and new_leftover fields.  It also destroys
* any associated hash table and host list.
*
* parsed_lines IN - Array of parsed lines to be cleaned up.
* max_parsed_lines IN - Number of elements in the parsed_lines array.
*/
static void cleanup_parsed_lines(parsed_line_t *parsed_lines, int max_parsed_lines) {
	if (parsed_lines == NULL || max_parsed_lines < 0) {
		return; 
	}
	for (int i = 0; i < max_parsed_lines; i++) {
		xfree(parsed_lines[i].key);
		xfree(parsed_lines[i].value);
		xfree(parsed_lines[i].new_leftover);
		if (parsed_lines[i].hashtbl) {
			s_p_hashtbl_destroy(parsed_lines[i].hashtbl); 
		}
		if (parsed_lines[i].hostlist) {
			hostlist_destroy(parsed_lines[i].hostlist); 
		}
	}
	xfree(parsed_lines);
}
#endif

/**
 * Counts the number of lines in the GRES configuration file, handling long lines and files without a trailing newline.
 * 
 * IN filename :Path to the GRES configuration file (e.g., "gres.conf").
 * RETURN :The total number of lines if successful, or -1 if an error occurs (e.g., file open failure).
 * 
 * note This function:
 *           1. Uses a buffer to read lines incrementally, supporting lines longer than the buffer size.
 *           2. Counts lines separated by newline characters ('\n').
 *           3. Handles files that do not end with a newline by counting the last partial line.
 *           4. Marks long lines that exceed the buffer size and ensures they are counted as a single line.
 */
static int count_gres_config_lines(const char *filename) {
	FILE *fg = fopen(filename, "r");
	if (!fg) {
		error("_gres_reconfig: unable to read \"%s\": %m", filename);
		return -1;
	}
	char buffer[4096];
	int max_parsed_lines = 0;
	size_t current_line_length = 0;
	bool is_long_line = false;

	while (fgets(buffer, sizeof(buffer), fg) != NULL) {
		size_t len = strlen(buffer);
		if (strchr(buffer, '\n') != NULL) {
			max_parsed_lines++;
			current_line_length = 0;
			is_long_line = false;
		} else {
			current_line_length += len;
			if (current_line_length >= sizeof(buffer) - 1) {
				is_long_line = true;
			}
		}
	}

	if (current_line_length > 0 || is_long_line) {
		max_parsed_lines++;
	}
	fclose(fg);
	return max_parsed_lines;
}

static void _gres_reconfig(void)
{
	node_record_t *node_ptr;
	char *gres_name;
	int i;

#ifdef __METASTACK_OPT_GRES_CONFIG
	struct timeval start, end;
	long seconds = 0, useconds = 0;
	double mtime = 0.0;
	char *gres_conf_file = NULL;
	int max_parsed_lines = 0;
	gres_conf_file = get_extra_conf_path("gres.conf");
	max_parsed_lines = count_gres_config_lines(gres_conf_file);
	if (slurm_conf.slurmctld_load_gres && max_parsed_lines >= 0) {
		int gres_number = -1;
		int num_parsed_lines = 0;
		slurmctld_load_gres_flag = true;
		gettimeofday(&start, NULL);
		max_parsed_lines++;

		parsed_lines = xmalloc(sizeof(parsed_line_t) * max_parsed_lines);
		if (parsed_lines == NULL) {
			error("_gres_reconfig:Memory allocation failed");
			xfree(gres_conf_file);
			return;
		}
		s_p_hashtbl_t *tbl = NULL;
		tbl = gres_parse_config_file(gres_conf_file, parsed_lines, max_parsed_lines, &num_parsed_lines);
		if (!tbl) {
			error("Failed to parse GRES configuration file");
			xfree(gres_conf_file); 
			return;
		}
		xfree(gres_conf_file);
		s_p_hashtbl_destroy(tbl);
		for (i = 0; (node_ptr = next_node(&i)); i++) {
			if (node_ptr->gres){
				gres_name = node_ptr->gres;
			}
			else{
				gres_name = node_ptr->config_ptr->gres;
			}
			gres_init_node_config(gres_name, &node_ptr->gres_list);
			if (gres_name == NULL) {
				continue;
			}
			gres_number = parse_gres_count(gres_name);
			if(gres_g_node_config_loadgres(
							node_ptr->config_ptr->cpus, node_ptr->name,
							node_ptr->gres_list, NULL, NULL, parsed_lines, gres_number) ==SLURM_ERROR){
				continue;
			}	
			gres_node_config_validate(
				node_ptr->name, node_ptr->config_ptr->gres,
				&node_ptr->gres, &node_ptr->gres_list,
				node_ptr->config_ptr->threads,
				node_ptr->config_ptr->cores,
				node_ptr->config_ptr->tot_sockets,
				slurm_conf.conf_flags & CONF_FLAG_OR, NULL);
		}
		if (parsed_lines != NULL) {
			cleanup_parsed_lines(parsed_lines, max_parsed_lines);
		}
		gettimeofday(&end, NULL);
		seconds = end.tv_sec - start.tv_sec;
		useconds = end.tv_usec - start.tv_usec;
		mtime = (seconds * 1000) + (useconds / 1000.0);
		slurmctld_load_gres_flag = false;
		debug("Slurmctld load Gres config time: %.2f ms\n", mtime);
	} else {
		for (i = 0; (node_ptr = next_node(&i)); i++) {
			if (node_ptr->gres)
				gres_name = node_ptr->gres;
			else
				gres_name = node_ptr->config_ptr->gres;
			gres_init_node_config(gres_name, &node_ptr->gres_list);
			if (!IS_NODE_CLOUD(node_ptr))
				continue;

			/*
			 * Load in GRES for node now. By default Slurm gets this
			 * information when the node registers for the first
			 * time, which can take a while for a node in the cloud
			 * to boot.
			 */
			gres_g_node_config_load(
				node_ptr->config_ptr->cpus, node_ptr->name,
				node_ptr->gres_list, NULL, NULL);
			
			gres_node_config_validate(
				node_ptr->name, node_ptr->config_ptr->gres,
				&node_ptr->gres, &node_ptr->gres_list,
				node_ptr->config_ptr->threads,
				node_ptr->config_ptr->cores,
				node_ptr->config_ptr->tot_sockets,
				slurm_conf.conf_flags & CONF_FLAG_OR, NULL);
		}
		xfree(gres_conf_file);
	}
#endif

}

/*
 * Append changeable features in old_features and not in features to features.
 */
static void _merge_changeable_features(char *old_features, char **features)
{
	char *save_ptr_old = NULL;
	char *tok_old, *tmp_old, *tok_new;
	char *sep;

	if (*features)
		sep = ",";
	else
		sep = "";

	/* Merge features strings, skipping duplicates */
	tmp_old = xstrdup(old_features);
	for (tok_old = strtok_r(tmp_old, ",", &save_ptr_old);
	     tok_old;
	     tok_old = strtok_r(NULL, ",", &save_ptr_old)) {
		bool match = false;

		if (!node_features_g_changeable_feature(tok_old))
			continue;

		if (*features) {
			char *tmp_new, *save_ptr_new = NULL;

			/* Check if old feature already exists in features string */
			tmp_new = xstrdup(*features);
			for (tok_new = strtok_r(tmp_new, ",", &save_ptr_new);
			     tok_new;
			     tok_new = strtok_r(NULL, ",", &save_ptr_new)) {
				if (!xstrcmp(tok_old, tok_new)) {
					match = true;
					break;
				}
			}
			xfree(tmp_new);
		}

		if (match)
			continue;

		xstrfmtcat(*features, "%s%s", sep, tok_old);
		sep = ",";
	}
	xfree(tmp_old);
}

static void _preserve_active_features(const char *available,
				      const char *old_active,
				      char **active)
{
	char *old_feature = NULL, *saveptr_old = NULL;
	char *tmp_old_active = NULL;

	if (!available || !old_active)
		return;

	tmp_old_active = xstrdup(old_active);
	for (old_feature = strtok_r(tmp_old_active, ",", &saveptr_old);
	     old_feature;
	     old_feature = strtok_r(NULL, ",", &saveptr_old)) {
		char *new_feature = NULL, *saveptr_avail = NULL;
		char *tmp_avail = NULL;

		if (!node_features_g_changeable_feature(old_feature))
			continue;

		tmp_avail = xstrdup(available);
		for (new_feature = strtok_r(tmp_avail, ",", &saveptr_avail);
		     new_feature;
		     new_feature = strtok_r(NULL, ",", &saveptr_avail)) {
			if (!xstrcmp(old_feature, new_feature)) {
				xstrfmtcat(*active, "%s%s",
					   *active ? "," : "", old_feature);
				break;
			}
		}
		xfree(tmp_avail);
	}
	xfree(tmp_old_active);
}

/*
 * Configure node features.
 * IN old_node_table_ptr IN - Previous nodes information
 * IN old_node_record_count IN - Count of previous nodes information
 * IN recover - replace node features data depending upon value.
 *              0, 1 - use data from config record, built using slurm.conf
 *              2 = use data from node record, built from saved state
 */
static void _set_features(node_record_t **old_node_table_ptr,
			  int old_node_record_count, int recover)
{
	node_record_t *node_ptr, *old_node_ptr;
	int i, node_features_cnt = node_features_g_count();

	for (i = 0; i < old_node_record_count; i++) {
		char *old_features_act;

		if (!(old_node_ptr = old_node_table_ptr[i]))
			continue;

		node_ptr  = find_node_record(old_node_ptr->name);

		if (node_ptr == NULL)
			continue;

		/*
		 * Load all from state, ignore what has been read from
		 * slurm.conf. Features in node record just a placeholder
		 * for restore_node_features() to set up new config records.
		 */
		if (recover == 2) {
			xfree(node_ptr->features);
			xfree(node_ptr->features_act);
			node_ptr->features = old_node_ptr->features;
			node_ptr->features_act = old_node_ptr->features_act;
			old_node_ptr->features = NULL;
			old_node_ptr->features_act = NULL;
			continue;
		}

		/* No changeable features so active == available */
		if (node_features_cnt == 0) {
			xfree(node_ptr->features_act);
			node_ptr->features_act = xstrdup(node_ptr->features);
			continue;
		}

		/* If we are here, there's a node_features plugin active */

		/*
		 * Changeable features may be listed in the slurm.conf along
		 * with the non-changeable features (e.g. cloud nodes). So
		 * filter out the changeable features and leave only the
		 * non-changeable features. non-changeable features are active
		 * by default.
		 */
		old_features_act = node_ptr->features_act;
		node_ptr->features_act =
			filter_out_changeable_features(node_ptr->features);

		/*
		 * Preserve active features on startup but make sure they are a
		 * subset of available features -- in case available features
		 * were changed.
		 *
		 * features_act has all non-changeable features now. We need to
		 * add back previous active features that are in available
		 * features.
		 *
		 * For cloud nodes, changeable features are added in slurm.conf.
		 * This will preserve the cloud active features on startup. When
		 * changeable features aren't defined in slurm.conf then
		 * features_act will be reset to all non-changeable features
		 * read in from slurm.conf and will expect to get the available
		 * and active features from the slurmd.
		 */
		_preserve_active_features(node_ptr->features, old_features_act,
					  &node_ptr->features_act);
		xfree(old_features_act);

		/*
		 * On startup, node_record_table_ptr is passed as
		 * old_node_table_ptr so no need to merge features.
		 */
		if (node_ptr == old_node_ptr)
			continue;

		/*
		 * The subset of plugin-controlled features_available
		 * and features_active found in the old node_ptr for this node
		 * are copied into new node respective fields.
		 * This will make that KNL modes are preserved while doing a
		 * reconfigure. Otherwise, we should wait until node is
		 * registered to get KNL available and active features.
		 */
		if (old_node_ptr->features != NULL) {
			_merge_changeable_features(old_node_ptr->features,
						   &node_ptr->features);
		}

		if (old_node_ptr->features_act != NULL) {
			_merge_changeable_features(old_node_ptr->features_act,
						   &node_ptr->features_act);
		}
	}
}

/*
 * _preserve_select_type_param - preserve original plugin parameters.
 *	Daemons and/or commands must be restarted for some
 *	select plugin value changes to take effect.
 * RET zero or error code
 */
static int _preserve_select_type_param(slurm_conf_t *ctl_conf_ptr,
                                       uint16_t old_select_type_p)
{
	int rc = SLURM_SUCCESS;

	/* SelectTypeParameters cannot change */
	if (old_select_type_p) {
		if (old_select_type_p != ctl_conf_ptr->select_type_param) {
			ctl_conf_ptr->select_type_param = old_select_type_p;
			rc = ESLURM_INVALID_SELECTTYPE_CHANGE;
		}
	}
	return rc;
}

/*
 * _sync_nodes_to_jobs - sync node state to job states on slurmctld restart.
 *	This routine marks nodes allocated to a job as busy no matter what
 *	the node's last saved state
 * RET count of nodes having state changed
 * Note: Operates on common variables, no arguments
 */
static int _sync_nodes_to_jobs(void)
{
	job_record_t *job_ptr;
	list_itr_t *job_iterator;
	int update_cnt = 0;

	job_iterator = list_iterator_create(job_list);
	while ((job_ptr = list_next(job_iterator))) {
		if (job_ptr->details && job_ptr->details->prolog_running) {
			job_ptr->details->prolog_running = 0;
			if (IS_JOB_CONFIGURING(job_ptr)) {
				prolog_slurmctld(job_ptr);
				(void) bb_g_job_begin(job_ptr);
			}
		}

		if (job_ptr->node_bitmap == NULL)
			;
		else if (IS_JOB_RUNNING(job_ptr) || IS_JOB_COMPLETING(job_ptr))
			update_cnt += _sync_nodes_to_active_job(job_ptr);
		else if (IS_JOB_SUSPENDED(job_ptr))
			_sync_nodes_to_suspended_job(job_ptr);
#ifdef __METASTACK_OPT_CACHE_QUERY
		_add_job_state_to_queue(job_ptr);
#endif
	}
	list_iterator_destroy(job_iterator);

	if (update_cnt) {
		info("_sync_nodes_to_jobs updated state of %d nodes",
		     update_cnt);
	}
	sync_front_end_state();
	return update_cnt;
}

/* For jobs which are in state COMPLETING, deallocate the nodes and
 * issue the RPC to kill the job */
static int _sync_nodes_to_comp_job(void)
{
	job_record_t *job_ptr;
	list_itr_t *job_iterator;
	int update_cnt = 0;

	job_iterator = list_iterator_create(job_list);
	while ((job_ptr = list_next(job_iterator))) {
		if ((job_ptr->node_bitmap) && IS_JOB_COMPLETING(job_ptr)) {

			/* If the controller is reconfiguring
			 * and the job is in completing state
			 * and the slurmctld epilog is already
			 * running which means deallocate_nodes()
			 * was alredy called, do invoke it again
			 * and don't start another epilog.
			 */
			if (job_ptr->epilog_running == true)
				continue;

			update_cnt++;
			info("%s: %pJ in completing state", __func__, job_ptr);
			if (!job_ptr->node_bitmap_cg)
				build_cg_bitmap(job_ptr);

			/* deallocate_nodes will remove this job from
			 * the system before it was added, so add it
			 * now
			 */
			if (accounting_enforce & ACCOUNTING_ENFORCE_LIMITS)
				acct_policy_job_begin(job_ptr, false);

			if (job_ptr->front_end_ptr)
				job_ptr->front_end_ptr->job_cnt_run++;
			deallocate_nodes(job_ptr, false, false, false);
			/* The job in completing state at slurmctld restart or
			 * reconfiguration, do not log completion again.
			 * job_completion_logger(job_ptr, false); */
#ifdef __METASTACK_OPT_CACHE_QUERY
			_add_job_state_to_queue(job_ptr);
#endif
		}
	}
	list_iterator_destroy(job_iterator);
	if (update_cnt)
		info("%s: completing %d jobs", __func__, update_cnt);
	return update_cnt;
}

/* Synchronize states of nodes and active jobs (RUNNING or COMPLETING state)
 * RET count of jobs with state changes */
static int _sync_nodes_to_active_job(job_record_t *job_ptr)
{
	int cnt = 0;
	uint32_t node_flags;
	node_record_t *node_ptr;
	bitstr_t *node_bitmap, *orig_job_node_bitmap;
	bool job_resized = false;

	if (job_ptr->node_bitmap_cg) /* job completing */
		node_bitmap = job_ptr->node_bitmap_cg;
	else
		node_bitmap = job_ptr->node_bitmap;

	orig_job_node_bitmap = bit_copy(job_ptr->job_resrcs->node_bitmap);

	job_ptr->node_cnt = bit_set_count(node_bitmap);
	for (int i = 0; (node_ptr = next_node_bitmap(node_bitmap, &i)); i++) {
		if ((job_ptr->details &&
		     (job_ptr->details->whole_node & WHOLE_NODE_USER)) ||
		    (job_ptr->part_ptr &&
		     (job_ptr->part_ptr->flags & PART_FLAG_EXCLUSIVE_USER))) {
			node_ptr->owner_job_cnt++;
			node_ptr->owner = job_ptr->user_id;
		}

		if (slurm_mcs_get_select(job_ptr) == 1) {
			xfree(node_ptr->mcs_label);
			node_ptr->mcs_label = xstrdup(job_ptr->mcs_label);
		}

		node_flags = node_ptr->node_state & NODE_STATE_FLAGS;

		if (IS_JOB_COMPLETING(job_ptr) && job_ptr->epilog_running) {
			/*
			 * _sync_nodes_to_comp_job() won't call
			 * deallocate_nodes()/make_node_comp() if the
			 * EpilogSlurmctld is still running to decrement
			 * run_job_cnt and increment comp_job_cnt, so just
			 * increment comp_job_cnt now.
			 */
			node_ptr->comp_job_cnt++;
		} else {
			/*
			 * run_job_cnt will be decremented by
			 * deallocate_nodes()/make_node_comp() in
			 * _sync_nodes_to_comp_job().
			 */
			node_ptr->run_job_cnt++;
		}

		if ((job_ptr->details) && (job_ptr->details->share_res == 0))
			node_ptr->no_share_job_cnt++;

		if (IS_NODE_DOWN(node_ptr)              &&
		    IS_JOB_RUNNING(job_ptr)             &&
		    (job_ptr->kill_on_node_fail == 0)   &&
		    (job_ptr->node_cnt > 1)) {
			/* This should only happen if a job was running
			 * on a node that was newly configured DOWN */
			int save_accounting_enforce;
			info("Removing failed node %s from %pJ",
			     node_ptr->name, job_ptr);

			/*
			 * Disable accounting here. Accounting reset for all
			 * jobs in _restore_job_accounting()
			 */
			save_accounting_enforce = accounting_enforce;
			accounting_enforce &= (~ACCOUNTING_ENFORCE_LIMITS);
			job_pre_resize_acctg(job_ptr);
			srun_node_fail(job_ptr, node_ptr->name);
			kill_step_on_node(job_ptr, node_ptr, true);
			excise_node_from_job(job_ptr, node_ptr);
			job_post_resize_acctg(job_ptr);
			job_resized = true;
			accounting_enforce = save_accounting_enforce;
		} else if (IS_NODE_DOWN(node_ptr) && IS_JOB_RUNNING(job_ptr)) {
			info("Killing %pJ on DOWN node %s",
			     job_ptr, node_ptr->name);
			_abort_job(job_ptr, JOB_NODE_FAIL, FAIL_DOWN_NODE,
				   NULL);
			cnt++;
		} else if (IS_NODE_IDLE(node_ptr)) {
			cnt++;
			node_ptr->node_state = NODE_STATE_ALLOCATED |
					       node_flags;
#ifdef __METASTACK_OPT_CACHE_QUERY
			_add_node_state_to_queue(node_ptr, true);
#endif
		}
	}

	/* If the job was resized then resize the bitmaps of the job's steps */
	if (job_resized) {
		rebuild_step_bitmaps(job_ptr, orig_job_node_bitmap);
	}
	FREE_NULL_BITMAP(orig_job_node_bitmap);

	if ((IS_JOB_RUNNING(job_ptr) || IS_JOB_SUSPENDED(job_ptr)) &&
	    (job_ptr->front_end_ptr != NULL))
		job_ptr->front_end_ptr->job_cnt_run++;

	set_initial_job_alias_list(job_ptr);

	return cnt;
}

/* Synchronize states of nodes and suspended jobs */
static void _sync_nodes_to_suspended_job(job_record_t *job_ptr)
{
	node_record_t *node_ptr;

	for (int i = 0; (node_ptr = next_node_bitmap(job_ptr->node_bitmap, &i));
	     i++) {
		node_ptr->sus_job_cnt++;
	}

	set_initial_job_alias_list(job_ptr);
}

/*
 * Build license_list for every job.
 * Reset accounting for every job.
 * Reset the running job count for scheduling policy.
 * This must be called after load_all_resv_state() and restore_node_features().
 */
static void _restore_job_accounting(void)
{
	job_record_t *job_ptr;
	list_itr_t *job_iterator;
	bool valid = true;
	List license_list;

	assoc_mgr_clear_used_info();

	job_iterator = list_iterator_create(job_list);
	while ((job_ptr = list_next(job_iterator))) {
		if (job_ptr->array_recs)
			job_ptr->array_recs->tot_run_tasks = 0;
	}

	list_iterator_reset(job_iterator);
	while ((job_ptr = list_next(job_iterator))) {
		(void) build_feature_list(job_ptr, false, false);
		(void) build_feature_list(job_ptr, true, false);

		if (job_ptr->details->features_use ==
		    job_ptr->details->features)
			job_ptr->details->feature_list_use =
				job_ptr->details->feature_list;
		else if (job_ptr->details->features_use ==
			 job_ptr->details->prefer)
			job_ptr->details->feature_list_use =
				job_ptr->details->prefer_list;
		(void) extra_constraints_parse(job_ptr->extra,
					       &job_ptr->extra_constraints);

		if (IS_JOB_RUNNING(job_ptr) || IS_JOB_SUSPENDED(job_ptr))
			job_array_start(job_ptr);

		if (accounting_enforce & ACCOUNTING_ENFORCE_LIMITS) {
			if (!IS_JOB_FINISHED(job_ptr))
				acct_policy_add_job_submit(job_ptr, false);
			if (IS_JOB_RUNNING(job_ptr) ||
			    IS_JOB_SUSPENDED(job_ptr)) {
				acct_policy_job_begin(job_ptr, false);
				job_claim_resv(job_ptr);
			} else if (IS_JOB_PENDING(job_ptr) &&
				   job_ptr->details &&
				   job_ptr->details->accrue_time) {
				/*
				 * accrue usage was cleared above with
				 * assoc_mgr_clear_used_info(). Clear accrue
				 * time so that _handle_add_accrue() will add
				 * the usage back.
				 */
				time_t save_accrue_time =
					job_ptr->details->accrue_time;
				job_ptr->details->accrue_time = 0;
				acct_policy_add_accrue_time(job_ptr, false);
				if (job_ptr->details->accrue_time)
					job_ptr->details->accrue_time =
						save_accrue_time;
			}
		}

		license_list = license_validate(job_ptr->licenses, false, false,
						job_ptr->tres_req_cnt, &valid);
		FREE_NULL_LIST(job_ptr->license_list);
		if (valid) {
			job_ptr->license_list = license_list;
			xfree(job_ptr->licenses);
			job_ptr->licenses =
				license_list_to_string(license_list);
		}

		if (IS_JOB_RUNNING(job_ptr) || IS_JOB_SUSPENDED(job_ptr))
			license_job_get(job_ptr, true);

	}
	list_iterator_destroy(job_iterator);
}

/* Flush accounting information on this cluster, then for each running or
 * suspended job, restore its state in the accounting system */
static void _acct_restore_active_jobs(void)
{
	job_record_t *job_ptr;
	list_itr_t *job_iterator;
	step_record_t *step_ptr;
	list_itr_t *step_iterator;

	info("Reinitializing job accounting state");
	acct_storage_g_flush_jobs_on_cluster(acct_db_conn,
					     time(NULL));
	job_iterator = list_iterator_create(job_list);
	while ((job_ptr = list_next(job_iterator))) {
		if (IS_JOB_SUSPENDED(job_ptr))
			jobacct_storage_g_job_suspend(acct_db_conn, job_ptr);
		if (IS_JOB_SUSPENDED(job_ptr) || IS_JOB_RUNNING(job_ptr)) {
			if (job_ptr->db_index != NO_VAL64)
				job_ptr->db_index = 0;
			step_iterator = list_iterator_create(
				job_ptr->step_list);
			while ((step_ptr = list_next(step_iterator))) {
				jobacct_storage_g_step_start(acct_db_conn,
							     step_ptr);
			}
			list_iterator_destroy (step_iterator);
		}
	}
	list_iterator_destroy(job_iterator);
}

extern int dump_config_state_lite(void)
{
	static int high_buffer_size = (1024 * 1024);
	int error_code = 0, log_fd;
	char *old_file = NULL, *new_file = NULL, *reg_file = NULL;
	buf_t *buffer = init_buf(high_buffer_size);

	DEF_TIMERS;

	START_TIMER;
	/* write header: version, time */
	pack16(SLURM_PROTOCOL_VERSION, buffer);
	pack_time(time(NULL), buffer);
	packstr(slurm_conf.accounting_storage_type, buffer);

	/* write the buffer to file */
	reg_file = xstrdup_printf("%s/last_config_lite",
	                          slurm_conf.state_save_location);
	old_file = xstrdup_printf("%s.old", reg_file);
	new_file = xstrdup_printf("%s.new", reg_file);

	log_fd = creat(new_file, 0600);
	if (log_fd < 0) {
		error("Can't save state, create file %s error %m",
		      new_file);
		error_code = errno;
	} else {
		int pos = 0, nwrite = get_buf_offset(buffer), amount;
		char *data = (char *)get_buf_data(buffer);
		high_buffer_size = MAX(nwrite, high_buffer_size);
		while (nwrite > 0) {
			amount = write(log_fd, &data[pos], nwrite);
			if ((amount < 0) && (errno != EINTR)) {
				error("Error writing file %s, %m", new_file);
				error_code = errno;
				break;
			}
			nwrite -= amount;
			pos    += amount;
		}
		fsync(log_fd);
		close(log_fd);
	}
	if (error_code)
		(void) unlink(new_file);
	else {			/* file shuffle */
		(void) unlink(old_file);
		if (link(reg_file, old_file))
			debug4("unable to create link for %s -> %s: %m",
			       reg_file, old_file);
		(void) unlink(reg_file);
		if (link(new_file, reg_file))
			debug4("unable to create link for %s -> %s: %m",
			       new_file, reg_file);
		(void) unlink(new_file);
	}
	xfree(old_file);
	xfree(reg_file);
	xfree(new_file);

	FREE_NULL_BUFFER(buffer);

	END_TIMER2(__func__);
	return error_code;
}

extern int load_config_state_lite(void)
{
	uint16_t ver = 0;
	char *state_file;
	buf_t *buffer;
	time_t buf_time;
	char *last_accounting_storage_type = NULL;

	/* Always ignore .old file */
	state_file = xstrdup_printf("%s/last_config_lite",
	                            slurm_conf.state_save_location);

	//info("looking at the %s file", state_file);
	if (!(buffer = create_mmap_buf(state_file))) {
		debug2("No last_config_lite file (%s) to recover", state_file);
		xfree(state_file);
		return ENOENT;
	}
	xfree(state_file);

	safe_unpack16(&ver, buffer);
	debug3("Version in last_conf_lite header is %u", ver);
	if (ver > SLURM_PROTOCOL_VERSION || ver < SLURM_MIN_PROTOCOL_VERSION) {
		if (!ignore_state_errors)
			fatal("Can not recover last_conf_lite, incompatible version, (%u not between %d and %d), start with '-i' to ignore this. Warning: using -i will lose the data that can't be recovered.",
			      ver, SLURM_MIN_PROTOCOL_VERSION,
			      SLURM_PROTOCOL_VERSION);
		error("***********************************************");
		error("Can not recover last_conf_lite, incompatible version, "
		      "(%u not between %d and %d)",
		      ver, SLURM_MIN_PROTOCOL_VERSION, SLURM_PROTOCOL_VERSION);
		error("***********************************************");
		FREE_NULL_BUFFER(buffer);
		return EFAULT;
	} else {
		safe_unpack_time(&buf_time, buffer);
		safe_unpackstr(&last_accounting_storage_type, buffer);
	}

	if (last_accounting_storage_type
	    && !xstrcmp(last_accounting_storage_type,
	                slurm_conf.accounting_storage_type))
		slurmctld_init_db = 0;
	xfree(last_accounting_storage_type);

	FREE_NULL_BUFFER(buffer);
	return SLURM_SUCCESS;

unpack_error:
	if (!ignore_state_errors)
		fatal("Incomplete last_config_lite checkpoint file, start with '-i' to ignore this. Warning: using -i will lose the data that can't be recovered.");
	error("Incomplete last_config_lite checkpoint file");
	FREE_NULL_BUFFER(buffer);

	return SLURM_ERROR;
}

#ifdef __METASTACK_OPT_CACHE_QUERY	

/*
 * Validate heterogeneous jobs
 *
 * Make sure that every active (not yet complete) job has all of its components
 * and they are all in the same state. Also rebuild het_job_list.
 * If hetjob is corrupted, aborts and removes it from job_list.
 */
extern void _validate_copy_het_jobs(void)
{
	list_itr_t *job_iterator;
	job_record_t *job_ptr = NULL, *het_job_ptr = NULL;
	hostset_t *hs;
	char *job_id_str = NULL;
	uint32_t job_id;
	bool het_job_valid;

	list_for_each(copy_job_list, _mark_het_job_unused, NULL);

	job_iterator = list_iterator_create(copy_job_list);
	while ((job_ptr = list_next(job_iterator))) {
		/* Checking for corrupted hetjob components */
		if (job_ptr->het_job_offset != 0) {
			het_job_ptr = find_hash_job_record(job_ptr->het_job_id, 1);
			if (!het_job_ptr) {
				continue;
			}
		}
		if ((job_ptr->het_job_id == 0) ||
			(job_ptr->het_job_offset != 0))
			continue;
		/* active het job leader found */
		FREE_NULL_LIST(job_ptr->het_job_list);
		job_id_str = NULL;
		/* Need to wrap numbers with brackets for hostset functions */
		xstrfmtcat(job_id_str, "[%s]", job_ptr->het_job_id_set);
		hs = hostset_create(job_id_str);
		xfree(job_id_str);
		if (!hs) {
			continue;
		}
		job_ptr->het_job_list = list_create(NULL);
		het_job_valid = true;	/* assume valid for now */
		while (het_job_valid && (job_id_str = hostset_shift(hs))) {
			job_id = (uint32_t) strtoll(job_id_str, NULL, 10);
			het_job_ptr = find_hash_job_record(job_id, 1);
			if (!het_job_ptr) {
				error("Could not find JobId=%u, part of hetjob JobId=%u",
					job_id, job_ptr->job_id);
				het_job_valid = false;
			} else if (het_job_ptr->het_job_id !=
				job_ptr->job_id) {
				error("Invalid state of JobId=%u, part of hetjob JobId=%u",
					job_id, job_ptr->job_id);
				het_job_valid = false;
			} else {
				list_append(job_ptr->het_job_list,
						het_job_ptr);
			}
			free(job_id_str);
		}
		hostset_destroy(hs);
		if (het_job_valid) {
			list_for_each(job_ptr->het_job_list, _mark_het_job_used,
					NULL);
		}
	}
	list_iterator_destroy(job_iterator);
}
#endif