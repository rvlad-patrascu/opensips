/*
 * Copyright (C) 2011 OpenSIPS Project
 *
 * This file is part of opensips, a free SIP server.
 *
 * opensips is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * opensips is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 *
 *
 * history:
 * ---------
 *  2015-07-07  created  by Marius Cristian Eseanu
 *	2016-06-xx 	rework (rvlad-patrascu)
 */

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include "../../sr_module.h"
#include "../../str.h"
#include "../../dprint.h"
#include "../../usr_avp.h"
#include "../../db/db.h"
#include "../../socket_info.h"
#include "../../resolve.h"
#include "../../mem/mem.h"
#include "../../mem/shm_mem.h"
#include "../../rw_locking.h"
#include "../../error.h"
#include "../../ut.h"
#include "../../mi/mi.h"
#include "../../timer.h"
#include "../../bin_interface.h"
#include "../../forward.h"
#include "clusterer.h"
#include "api.h"

#define RETRIED_IS_WAITING_FOR_REPLY(_link_state) \
	((_link_state) == LS_RESTARTED || (_link_state) == LS_RETRYING)

#define check_val( _col, _val, _type, _not_null, _is_empty_str) \
	do { \
		if ((_val)->type!=_type) { \
			LM_ERR("column %.*s has a bad type\n", _col.len, _col.s); \
			goto error; \
		} \
		if (_not_null && (_val)->nul) { \
			LM_ERR("column %.*s is null\n", _col.len, _col.s); \
			goto error; \
		} \
		if (_is_empty_str && !VAL_STRING(_val)) { \
			LM_ERR("column %.*s (str) is empty\n", _col.len, _col.s); \
			goto error; \
		} \
	} while (0)

static rw_lock_t *ref_lock;

struct clusterer_binds clusterer_api;

/* DB */
static db_con_t *db_hdl;
static db_func_t dr_dbf;

static str clusterer_db_url = {NULL, 0};
static str db_table = str_init("clusterer");
static str id_col = str_init("id");	/* PK column */
static str cluster_id_col = str_init("cluster_id");
static str node_id_col = str_init("node_id");
static str url_col = str_init("url");
static str state_col = str_init("state");
static str no_ping_retries_col = str_init("no_ping_retries");
static str priority_col = str_init("priority");
static str description_col = str_init("description");

static db_op_t op_eq = OP_EQ;
static db_key_t *clusterer_cluster_id_key;
static db_val_t *clusterer_cluster_id_value;

int current_id = -1;

static int ping_interval = DEFAULT_PING_INTERVAL;
static int node_timeout = DEFAULT_NODE_TIMEOUT;
static int ping_timeout = DEFAULT_PING_TIMEOUT;

static cluster_info_t **cluster_list;
static struct mod_registration *clusterer_reg_modules;

/* initialize functions */
static int mod_init(void);
static int child_init(int rank);

/* destroy function */
static void destroy(void);

/* MI functions */
static struct mi_root* clusterer_reload(struct mi_root* root, void *param);
static struct mi_root* clusterer_set_status(struct mi_root *cmd, void *param);
static struct mi_root * clusterer_list(struct mi_root *root, void *param);

static inline void heartbeats_timer(void);
static void heartbeats_timer_handler(unsigned int ticks, void *param);
static void heartbeats_utimer_handler(utime_t ticks, void *param);
static void receive_clusterer_bin_packets(int packet_type, struct receive_info *ri, void *att);

cluster_info_t* load_db_info(db_func_t *dr_dbf, db_con_t* db_hdl, str *db_table);
void free_info(cluster_info_t *cl_list);
static node_info_t *clusterer_find_nodes(int cluster_id);
static int set_link(clusterer_link_state new_ls, node_info_t *node_a, node_info_t *node_b);

static inline int gcd(int a, int b) {
	int t;

	while(b) {
		t = a;
		a = b;
		b = t % b;
	}

	return a;
}

/*
 * Exported functions
 */
static cmd_export_t cmds[]={
	{"load_clusterer",  (cmd_function)load_clusterer, 0, 0, 0, 0},
	{0,0,0,0,0,0}
};

/*
 * Exported parameters
 */
static param_export_t params[] = {
	{"db_url",				STR_PARAM,	&clusterer_db_url.s		},
	{"db_table",			STR_PARAM,	&db_table.s		},
	{"current_id",			INT_PARAM,	&current_id		},
	{"ping_interval",		INT_PARAM,	&ping_interval	},
	{"node_timeout",		INT_PARAM,	&node_timeout	},
	{"ping_timeout",		INT_PARAM,	&ping_timeout	},
	{"cluster_id_col",		STR_PARAM,	&cluster_id_col.s	},
	{"node_id_col",			STR_PARAM,	&node_id_col.s	},
	{"id_col",				STR_PARAM,	&id_col.s	},
	{"state_col",			STR_PARAM,	&state_col.s		},
	{"url_col",				STR_PARAM,	&url_col.s		},
	{"description_col",		STR_PARAM,	&description_col.s	},
	{0, 0, 0}
};

/*
 * Exported MI functions
 */	
static mi_export_t mi_cmds[] = {
	{ "clusterer_reload", "reloads stored data from the database", clusterer_reload, 0, 0, 0},
	{ "clusterer_set_status", "sets the status for a specified connection", clusterer_set_status, 0, 0, 0},
	{ "clusterer_list", "lists the available connections for the specified server", clusterer_list, 0, 0, 0},
	{0, 0, 0, 0, 0, 0}
};

static dep_export_t deps = {
	{ /* OpenSIPS module dependencies */
		{ MOD_TYPE_SQLDB, NULL, DEP_ABORT },
		{ MOD_TYPE_NULL, NULL, 0 },
	},
	{ /* modparam dependencies */
		{ NULL, NULL },
	},
};

/**
 * module exports
 */
struct module_exports exports= {
	"clusterer",			/* module name */
	MOD_TYPE_DEFAULT,/* class of this module */
	MODULE_VERSION,
	DEFAULT_DLFLAGS,			/* dlopen flags */
	&deps,            /* OpenSIPS module dependencies */
	cmds,							/* exported functions */
	0,							/* exported async functions */
	params,							/* exported parameters */
	0,							/* exported statistics */
	mi_cmds,							/* exported MI functions */
	0,							/* exported pseudo-variables */
	0,						/* extra processes */
	mod_init,					/* module initialization function */
	0,							/* response handling function */
	destroy,					/* destroy function */
	child_init					/* per-child init function */
};

static int mod_init(void)
{
	int heartbeats_timer_interval;

	LM_INFO("Clusterer initializing\n");

	init_db_url(clusterer_db_url, 0 /*cannot be null*/);

	if (current_id < 1) {
		LM_CRIT("invalid current_id parameter\n");
		return -1;
	}
	if (ping_interval <= 0) {
		LM_WARN("invalid ping_interval parameter, using default value\n");
		ping_interval = DEFAULT_PING_INTERVAL;
	}
	if (node_timeout < 0) {
		LM_WARN("invalid node_timeout parameter, using default value\n");
		node_timeout = DEFAULT_NODE_TIMEOUT;
	}
	if (ping_timeout <= 0) {
		LM_WARN("invalid ping_timeout parameter, using default value\n");
		ping_timeout = DEFAULT_PING_TIMEOUT;
	}

	/* create & init lock */
	if ((ref_lock = lock_init_rw()) == NULL) {
		LM_CRIT("failed to init lock\n");
		goto error;
	}

	/* data pointer in shm */
	cluster_list = shm_malloc(sizeof *cluster_list);
	if (!cluster_list) {
		LM_CRIT("No more shm memory\n");
		goto error;
	}
	*cluster_list = NULL;

	/* bind to the mysql module */
	if (db_bind_mod(&clusterer_db_url, &dr_dbf)) {
		LM_CRIT("cannot bind to database module! "
			"Did you forget to load a database module ?\n");
		goto error;
	}

	if (!DB_CAPABILITY(dr_dbf, DB_CAP_QUERY)) {
		LM_CRIT("given SQL DB does not provide query types needed by this module!\n");
		goto error;
	}

	/* register timer */
	heartbeats_timer_interval = gcd(ping_interval*1000, ping_timeout);
	heartbeats_timer_interval = gcd(heartbeats_timer_interval, node_timeout*1000);

	LM_DBG("Registerd heartbeats timer at %d ms\n", heartbeats_timer_interval);

	if (heartbeats_timer_interval % 1000 == 0) {
		if (register_timer("clstr-heartbeats-timer", heartbeats_timer_handler,
			NULL, heartbeats_timer_interval/1000, TIMER_FLAG_DELAY_ON_DELAY) < 0) {
			LM_CRIT("unable to register clusterer heartbeats timer\n");
			goto error;
		}
	} else {
		if (register_utimer("clstr-heartbeats-utimer", heartbeats_utimer_handler,
			NULL, heartbeats_timer_interval*1000, TIMER_FLAG_DELAY_ON_DELAY) < 0) {
			LM_CRIT("unable to register clusterer heartbeats timer\n");
			goto error;
		}
	}

	if (bin_register_cb("clusterer", receive_clusterer_bin_packets, NULL) < 0) {
		LM_CRIT("Cannot register clusterer binary packet callback!\n");
		goto error;
	}

	return 0;

error:
	if (ref_lock) {
		lock_destroy_rw(ref_lock);
		ref_lock = 0;
	}
	if (cluster_list) {
		shm_free(cluster_list);
		cluster_list = 0;
	}
	return -1;
}

/* initialize child */
static int child_init(int rank)
{
	if (rank == PROC_TCP_MAIN || rank == PROC_BIN)
		return 0;

	/* init DB connection */
	if ((db_hdl = dr_dbf.init(&clusterer_db_url)) == 0) {
		LM_ERR("cannot initialize database connection\n");
		return -1;
	}

	/* child 1 loads the clusterer DB info */
	if (rank == 1) {
		*cluster_list = load_db_info(&dr_dbf, db_hdl, &db_table);
		if (*cluster_list == NULL) {
			LM_ERR("Failed to load info from DB\n");
			return -1;
		}
	}

	return 0;
}

static void heartbeats_timer_handler(unsigned int ticks, void *param) {
	heartbeats_timer();
}

static void heartbeats_utimer_handler(utime_t ticks, void *param) {
	heartbeats_timer();
}

static int send_heartbeat_msg(int cluster_id, union sockaddr_union* dest, clusterer_msg_type type) {
	static str module_name = str_init("clusterer");
	str send_buffer;

	if (bin_init(&module_name, type, BIN_VERSION) < 0) {
		LM_ERR("Failed to init bin send buffer\n");
		return -1;
	}

	bin_push_int(cluster_id);
	bin_push_int(current_id);
	bin_get_buffer(&send_buffer);

	if (msg_send(NULL, PROTO_BIN, dest, 0, send_buffer.s, send_buffer.len, 0) != 0)
		return -1;

	return 0;
}

static inline void heartbeats_timer(void) {
	struct timeval now;
	utime_t last_ping_int;
	utime_t ping_reply_int;
	cluster_info_t *clusters_it;
	node_info_t *node;

	gettimeofday(&now, NULL);

	lock_start_write(ref_lock);

	for (clusters_it = *cluster_list; clusters_it; clusters_it = clusters_it->next) {
		if (!clusters_it->current_node->enabled)
			continue;

		for(node = clusters_it->node_list; node; node = node->next) {
			if (!node->enabled)
				continue;

			/* currently only support PROTO_BIN */
			if (node->proto != PROTO_BIN)
				continue;

			/* restart pinging sequence */
			if (node->link_state == LS_RESTART_PINGING) {
				node->last_ping = now;
				if (send_heartbeat_msg(clusters_it->cluster_id, &node->addr, CLUSTERER_PING) < 0) {
					LM_ERR("Failed to send ping to node %d\n", node->node_id);
					if (node->no_ping_retries == 0)
						set_link(LS_DOWN, clusters_it->current_node, node);
					else {
						node->curr_no_retries = node->no_ping_retries;
						set_link(LS_RETRY_SEND_FAIL, clusters_it->current_node, node);
					}
				} else {
					set_link(LS_RESTARTED, clusters_it->current_node, node);
					LM_DBG("Sent ping to node %d\n", node->node_id);
				}
				continue;
			}

			ping_reply_int = node->last_pong.tv_sec*1000000 + node->last_pong.tv_usec
				- node->last_ping.tv_sec*1000000 - node->last_ping.tv_usec;
			last_ping_int = now.tv_sec*1000000 + now.tv_usec
				- node->last_ping.tv_sec*1000000 - node->last_ping.tv_usec;

			if (node->link_state == LS_RETRY_SEND_FAIL &&
				last_ping_int >= ping_timeout*1000) {
				node->last_ping = now;
				node->curr_no_retries--;
				if (send_heartbeat_msg(clusters_it->cluster_id, &node->addr, CLUSTERER_PING) < 0) {
					LM_ERR("Failed to send ping retry to node %d\n", node->node_id);
					if (node->curr_no_retries == 0) {
						set_link(LS_DOWN, clusters_it->current_node, node);
						LM_WARN("Maximum number of retries reached, node %d is down\n",
							node->node_id);
					}
				} else {
					LM_DBG("Sent ping to node %d\n", node->node_id);
					set_link(LS_RETRYING, clusters_it->current_node, node);
				}
				continue;
			}

			/* send first ping retry */
			if ((node->link_state == LS_UP ||
				node->link_state == LS_RESTARTED) &&
				(ping_reply_int >= ping_timeout*1000 || ping_reply_int <= 0) &&
				last_ping_int >= ping_timeout*1000) {
				if (node->no_ping_retries == 0) {
					set_link(LS_DOWN, clusters_it->current_node, node);
					LM_WARN("Pong not received, node %d is down\n", node->node_id);
				} else {
					LM_WARN("Pong not received, node %d is possibly down, retrying\n",
						node->node_id);
					node->last_ping = now;
					if (send_heartbeat_msg(clusters_it->cluster_id, &node->addr, CLUSTERER_PING) < 0) {
						LM_ERR("Failed to send ping to node %d\n", node->node_id);
						node->curr_no_retries = node->no_ping_retries;
						set_link(LS_RETRY_SEND_FAIL, clusters_it->current_node, node);
					} else {
						LM_DBG("Sent ping retry to node %d\n", node->node_id);
						set_link(LS_RETRYING, clusters_it->current_node, node);
						node->curr_no_retries = --node->no_ping_retries;
					}
					continue;
				}
			}

			/* previous ping retry not replied, continue to retry */
			if (node->link_state == LS_RETRYING &&
				(ping_reply_int >= ping_timeout*1000 || ping_reply_int <= 0) &&
				last_ping_int >= ping_timeout*1000) {
				if (node->curr_no_retries > 0) {
					node->last_ping = now;
					if (send_heartbeat_msg(clusters_it->cluster_id, &node->addr, CLUSTERER_PING) < 0) {
						LM_ERR("Failed to send ping retry to node %d\n",
							node->node_id);
						set_link(LS_RETRY_SEND_FAIL, clusters_it->current_node, node);
					} else {
						LM_DBG("Sent ping retry to node %d\n", node->node_id);
						node->curr_no_retries--;
					}
					continue;
				} else {
					set_link(LS_DOWN, clusters_it->current_node, node);
					LM_WARN("Pong not received, node %d is down\n", node->node_id);
				}
			}

			/* ping a failed node after node_timeout since last ping */
			if (node->link_state == LS_DOWN && last_ping_int >= node_timeout*1000000) {
				LM_INFO("Node timeout passed, restart pinging node %d\n",
					node->node_id);

				node->last_ping = now;
				if (send_heartbeat_msg(clusters_it->cluster_id, &node->addr, CLUSTERER_PING) < 0) {
					LM_ERR("Failed to send ping to node %d\n", node->node_id);
					if (node->no_ping_retries == 0)
						set_link(LS_DOWN, clusters_it->current_node, node);
					else {
						node->curr_no_retries = node->no_ping_retries;
						set_link(LS_RETRY_SEND_FAIL, clusters_it->current_node, node);
					}
				} else {
					set_link(LS_RESTARTED, clusters_it->current_node, node);
					LM_DBG("Sent ping to node %d\n", node->node_id);
				}
				continue;
			}

			/* send regular ping */
			if (node->link_state == LS_UP && last_ping_int >= ping_interval*1000000) {
				node->last_ping = now;
				if (send_heartbeat_msg(clusters_it->cluster_id, &node->addr, CLUSTERER_PING) < 0) {
					LM_ERR("Failed to send ping to node %d\n", node->node_id);
					if (node->no_ping_retries == 0)
						set_link(LS_DOWN, clusters_it->current_node, node);
					else {
						node->curr_no_retries = node->no_ping_retries;
						set_link(LS_RETRY_SEND_FAIL, clusters_it->current_node, node);
					}
				} else
					LM_DBG("Sent ping to node %d\n", node->node_id);
			}
		}
	}

	lock_stop_write(ref_lock);
}

int add_node_info(cluster_info_t **cl_list, int *int_vals, char **str_vals)
{
	char *host;
	int hlen, port;
	struct hostent *he;
	int cluster_id;
	cluster_info_t *cluster = NULL;
	node_info_t *new_info = NULL;
	struct timeval t;
	str st;
	struct mod_registration *mod;
	struct cluster_mod *new_cl_mod = NULL;

	cluster_id = int_vals[INT_VALS_CLUSTER_ID_COL];

	for (cluster = *cl_list; cluster && cluster->cluster_id != cluster_id;
		cluster = cluster->next) ;

	if (!cluster) {
		cluster = shm_malloc(sizeof *cluster);
		if (!cluster) {
			LM_ERR("no more shm memory\n");
			goto error;
		}

		/* look for modules that registered for this cluster */
		for (mod = clusterer_reg_modules; mod; mod = mod->next) {
			if (mod->accept_cluster_id == cluster_id) {
				new_cl_mod = shm_malloc(sizeof *new_cl_mod);
				if (!new_cl_mod) {
					LM_ERR("No more shm memory\n");
					goto error;
				}

				new_cl_mod->reg = mod;
				new_cl_mod->next = cluster->modules;
				cluster->modules = new_cl_mod;
			}
		}

		cluster->cluster_id = cluster_id;
		cluster->node_list = NULL;
		cluster->current_node = NULL;
		cluster->next = *cl_list;
		*cl_list = cluster;
	}

	new_info = shm_malloc(sizeof *new_info);
	if (!new_info) {
		LM_ERR("no more shm memory\n");
		goto error;
	}

	new_info->id = int_vals[INT_VALS_ID_COL];
	new_info->node_id = int_vals[INT_VALS_NODE_ID_COL];
	new_info->enabled = int_vals[INT_VALS_STATE_COL];

	if (int_vals[INT_VALS_NODE_ID_COL] != current_id)
		new_info->link_state = LS_RESTART_PINGING;
	else
		new_info->link_state = LS_UP;

	if (strlen(str_vals[STR_VALS_DESCRIPTION_COL]) != 0) {
		new_info->description.len = strlen(str_vals[STR_VALS_DESCRIPTION_COL]);
		new_info->description.s = shm_malloc(new_info->description.len * sizeof(char));
		if (new_info->description.s == NULL) {
			LM_ERR("no more shm memory\n");
			goto error;
		}
		memcpy(new_info->description.s, str_vals[STR_VALS_DESCRIPTION_COL],
			new_info->description.len);
	} else {
		new_info->description.s = NULL;
		new_info->description.len = 0;
	}

	if (str_vals[STR_VALS_URL_COL] == NULL) {
		LM_ERR("no url specified in DB\n");
		goto error;
	}
	new_info->url.len = strlen(str_vals[STR_VALS_URL_COL]);
	new_info->url.s = shm_malloc(strlen(str_vals[STR_VALS_URL_COL]) * sizeof(char));
	if (!new_info->url.s) {
		LM_ERR("no more shm memory\n");
		goto error;
	}
	memcpy(new_info->url.s, str_vals[STR_VALS_URL_COL], new_info->url.len);

	if (int_vals[INT_VALS_NODE_ID_COL] != current_id) {
		if (parse_phostport(new_info->url.s, new_info->url.len, &host, &hlen, &port, &new_info->proto) < 0) {
			LM_ERR("Bad URL!\n");
			goto error;
		}
		if (new_info->proto == PROTO_NONE)
			new_info->proto = PROTO_BIN;

		st.s = host;
		st.len = hlen;
		he = sip_resolvehost(&st, (unsigned short *) &port,
			(unsigned short *) &new_info->proto, 0, 0);
		if (!he) {
			LM_ERR("Cannot resolve host: %.*s\n", hlen, host);
			goto error;
		}

		hostent2su(&new_info->addr, he, 0, port);

		t.tv_sec = 0;
		t.tv_usec = 0;
		new_info->last_ping = t;
		new_info->last_pong = t;
	}

	new_info->priority = int_vals[INT_VALS_PRIORITY_COL];

	new_info->no_ping_retries = int_vals[INT_VALS_NO_PING_RETRIES_COL];
	new_info->curr_no_retries = 0;

	new_info->cluster = cluster;

	new_info->neighbour_list = NULL;
	new_info->sp_top_version = 0;
	new_info->next_hop = NULL;
	new_info->sp_info = shm_malloc(sizeof(struct node_search_info));
	if (!new_info->sp_info) {
		LM_ERR("no more shm memory\n");
		goto error;
	}
	new_info->sp_info->node = new_info;

	if (int_vals[INT_VALS_NODE_ID_COL] != current_id) {
		new_info->next = cluster->node_list;
		cluster->node_list = new_info;
	} else {
		new_info->next = NULL;
		cluster->current_node = new_info;
	}

	return 0;
error:
	if (new_info) {
		if (new_info->description.s)
			shm_free(new_info->description.s);

		if (new_info->url.s)
			shm_free(new_info->url.s);

		if (new_info->sp_info)
			shm_free(new_info->sp_info);

		shm_free(new_info);
	}
	return -1;
}

/* loads info from the db */
cluster_info_t* load_db_info(db_func_t *dr_dbf, db_con_t* db_hdl, str *db_table)
{
	int int_vals[NO_DB_INT_VALS];
	char *str_vals[NO_DB_STR_VALS];
	int no_clusters;
	int i;
	db_key_t columns[NO_DB_COLS];	/* the columns from the db table */
	db_res_t *res = NULL;
	db_row_t *row;
	cluster_info_t *cl_list = NULL;
	static db_key_t clusterer_node_id_key = &node_id_col;
	static db_val_t clusterer_node_id_value = {
		.type = DB_INT,
		.nul = 0,
	};

	columns[0] = &id_col;
	columns[1] = &cluster_id_col;
	columns[2] = &node_id_col;
	columns[3] = &url_col;
	columns[4] = &state_col;
	columns[5] = &no_ping_retries_col;
	columns[6] = &priority_col;
	columns[7] = &description_col;

	CON_OR_RESET(db_hdl);

	if (db_check_table_version(dr_dbf, db_hdl, db_table, 1/*version*/) != 0)
		goto error;

	if (dr_dbf->use_table(db_hdl, db_table) < 0) {
		LM_ERR("cannot select table \"%.*s\"\n", db_table->len, db_table->s);
		goto error;
	}

	LM_DBG("DB query - retrieve the list of clusters"
		" in which the current node runs\n");

	VAL_INT(&clusterer_node_id_value) = current_id;

	/* first we see in which clusters the current node runs*/
	if (dr_dbf->query(db_hdl, &clusterer_node_id_key, &op_eq,
		&clusterer_node_id_value, columns+1, 1, 1, 0, &res) < 0) {
		LM_ERR("DB query failed - cannot retrieve the list of clusters in which"
			" the current node runs\n");
		goto error;
	}

	LM_DBG("%d rows found in %.*s\n",
		RES_ROW_N(res), db_table->len, db_table->s);

	if (RES_ROW_N(res) == 0) {
		LM_WARN("No nodes found in cluster\n");
		return NULL;
	}

	clusterer_cluster_id_key = pkg_realloc(clusterer_cluster_id_key,
		RES_ROW_N(res) * sizeof(db_key_t));
	if (!clusterer_cluster_id_key) {
		LM_ERR("no more pkg memory\n");
		goto error;
	}

	for (i = 0; i < RES_ROW_N(res); i++)
		clusterer_cluster_id_key[i] = &cluster_id_col;

	clusterer_cluster_id_value = pkg_realloc(clusterer_cluster_id_value,
		RES_ROW_N(res) * sizeof(db_val_t));
	if (!clusterer_cluster_id_value) {
		LM_ERR("no more pkg memory\n");
		goto error;
	}

	for (i = 0; i < RES_ROW_N(res); i++) {
		VAL_TYPE(clusterer_cluster_id_value + i) = DB_INT;
		VAL_NULL(clusterer_cluster_id_value + i) = 0;
	}

	for (i = 0; i < RES_ROW_N(res); i++) {
		row = RES_ROWS(res) + i;
		check_val(cluster_id_col, ROW_VALUES(row), DB_INT, 1, 0);
		VAL_INT(clusterer_cluster_id_value + i) = VAL_INT(ROW_VALUES(row));
	}

	no_clusters = RES_ROW_N(res);
	dr_dbf->free_result(db_hdl, res);
	res = NULL;

	LM_DBG("DB query - retrieve nodes info\n");

	CON_USE_OR_OP(db_hdl);

	if (dr_dbf->query(db_hdl, clusterer_cluster_id_key, 0,
		clusterer_cluster_id_value, columns, no_clusters, NO_DB_COLS, 0, &res) < 0) {
		LM_ERR("DB query failed - retrieve valid connections\n");
		goto error;
	}

	LM_DBG("%d rows found in %.*s\n",
		RES_ROW_N(res), db_table->len, db_table->s);

	for (i = 0; i < RES_ROW_N(res); i++) {
		row = RES_ROWS(res) + i;

		check_val(id_col, ROW_VALUES(row), DB_INT, 1, 0);
		int_vals[INT_VALS_ID_COL] = VAL_INT(ROW_VALUES(row));
		LM_DBG("id: %d\n", int_vals[INT_VALS_ID_COL]);

		check_val(cluster_id_col, ROW_VALUES(row) + 1, DB_INT, 1, 0);
		int_vals[INT_VALS_CLUSTER_ID_COL] = VAL_INT(ROW_VALUES(row) + 1);
		LM_DBG("cluster_id: %d\n", int_vals[INT_VALS_CLUSTER_ID_COL]);

		check_val(node_id_col, ROW_VALUES(row) + 2, DB_INT, 1, 0);
		int_vals[INT_VALS_NODE_ID_COL] = VAL_INT(ROW_VALUES(row) + 2);
		LM_DBG("node_id: %d\n", int_vals[INT_VALS_NODE_ID_COL]);

		check_val(url_col, ROW_VALUES(row) + 3, DB_STRING, 1, 1);
		str_vals[STR_VALS_URL_COL] = (char*) VAL_STRING(ROW_VALUES(row) + 3);
		LM_DBG("url: %s\n", str_vals[STR_VALS_URL_COL]);

		check_val(state_col, ROW_VALUES(row) + 4, DB_INT, 1, 0);
		int_vals[INT_VALS_STATE_COL] = VAL_INT(ROW_VALUES(row) + 4);
		LM_DBG("state: %d\n", int_vals[INT_VALS_STATE_COL]);

		check_val(no_ping_retries_col, ROW_VALUES(row) + 5, DB_INT, 1, 0);
		int_vals[INT_VALS_NO_PING_RETRIES_COL] = VAL_INT(ROW_VALUES(row) + 5);
		LM_DBG("no_ping_retries: %d\n", int_vals[INT_VALS_NO_PING_RETRIES_COL]);

		check_val(priority_col, ROW_VALUES(row) + 6, DB_INT, 1, 0);
		int_vals[INT_VALS_PRIORITY_COL] = VAL_INT(ROW_VALUES(row) + 6);
		LM_DBG("priority: %d\n", int_vals[INT_VALS_PRIORITY_COL]);

		check_val(description_col, ROW_VALUES(row) + 7, DB_STRING, 0, 0);
		str_vals[STR_VALS_DESCRIPTION_COL] = (char*) VAL_STRING(ROW_VALUES(row) + 7);
		LM_DBG("description: %s\n", str_vals[STR_VALS_DESCRIPTION_COL]);

		/* add info to backing list */
		if (add_node_info(&cl_list, int_vals, str_vals) < 0) {
			LM_ERR("Unable to add node info to backing list\n");
			goto error;
		}
	}

	if (RES_ROW_N(res) == 1)
		LM_WARN("The node is the only one in the cluster\n");

	dr_dbf->free_result(db_hdl, res);

	return cl_list;

error:
	if (res)
		dr_dbf->free_result(db_hdl, res);
	if (cl_list)
		free_info(cl_list);
	return NULL;
}

void free_info(cluster_info_t *cl_list)
{
	cluster_info_t *tmp_cl;
	node_info_t *info, *tmp_info;
	struct cluster_mod *cl_m, *tmp_cl_m;

	while (cl_list != NULL) {
		tmp_cl = cl_list;
		cl_list = cl_list->next;

		info = tmp_cl->node_list;
		while (info != NULL) {
			if (info->url.s)
				shm_free(info->url.s);
			if (info->description.s)
				shm_free(info->description.s);
			tmp_info = info;
			info = info->next;
			shm_free(tmp_info);
		}

		cl_m = tmp_cl->modules;
		while (cl_m != NULL) {
			tmp_cl_m = cl_m;
			cl_m = cl_m->next;
			shm_free(tmp_cl_m);
		}

		shm_free(tmp_cl);
	}
}

static void destroy(void)
{
	struct mod_registration *tmp;

	/* close DB connection */
	if (db_hdl) {
		dr_dbf.close(db_hdl);
		db_hdl = NULL;
	}

	/* destroy data */
	if (cluster_list) {
		if (*cluster_list)
			free_info(*cluster_list);
		shm_free(cluster_list);
		cluster_list = NULL;
	}

	while (clusterer_reg_modules) {
		tmp = clusterer_reg_modules;
		clusterer_reg_modules = clusterer_reg_modules->next;
		shm_free(tmp);
	}

	/* destroy lock */
	if (ref_lock) {
		lock_destroy_rw(ref_lock);
		ref_lock = NULL;
	}
}

static struct mi_root* clusterer_reload(struct mi_root* root, void *param)
{
	cluster_info_t *new_info;
	cluster_info_t *old_info;

	LM_INFO("reload data MI command received!\n");

	new_info = load_db_info(&dr_dbf, db_hdl, &db_table);
	if (!new_info) {
		LM_ERR("Failed to load info from DB\n");
		return init_mi_tree(500, "Failed to reload", 16);
	}

	lock_start_write(ref_lock);
	old_info = *cluster_list;
	*cluster_list = new_info;
	lock_stop_write(ref_lock);

	if (old_info)
		free_info(old_info);

	return init_mi_tree(200, MI_SSTR(MI_OK));
}

static int set_state(int cluster_id, int node_id, enum cl_node_state state)
{
	cluster_info_t *cluster = NULL;
	node_info_t *node;

	lock_start_write(ref_lock);

	for (cluster = *cluster_list; cluster; cluster = cluster->next)
		if (cluster->cluster_id == cluster_id)
			break;
	if (!cluster) {
		lock_stop_write(ref_lock);
		LM_ERR("Cluster id %d not found\n", cluster_id);
		return -1;
	}

	if (node_id == current_id) {
		cluster->current_node->enabled = state;

		lock_stop_write(ref_lock);

		LM_INFO("Set state: %s for node id: %d from cluster id: %d\n",
			state ? "enabled" : "disabled", node_id, cluster_id);
		return 0;
	}

	node = clusterer_find_nodes(cluster_id);
	for (; node; node = node->next)
		if (node->node_id == node_id)
			break;
	if (!node) {
		lock_stop_write(ref_lock);
		LM_ERR("Node id: %d not found in cluster\n", node_id);
		return -2;
	}

	if (state == STATE_ENABLED && !node->enabled)
		set_link(LS_RESTART_PINGING, cluster->current_node, node);
	if (state == STATE_DISABLED && node->enabled)
		set_link(LS_DOWN, cluster->current_node, node);

	node->enabled = state;

	lock_stop_write(ref_lock);

	LM_INFO("Set state: %s for node id: %d from cluster id: %d\n",
			state ? "enabled" : "disabled", node_id, cluster_id);
	return 0;
}

static struct mi_root* clusterer_set_status(struct mi_root *cmd, void *param)
{
	unsigned int cluster_id;
	unsigned int node_id;
	unsigned int state;
	int rc;
	struct mi_node *node;

	LM_INFO("set status MI command received!\n");

	node = cmd->node.kids;

	if (node == NULL || node->next == NULL || node->next->next==NULL ||
		node->next->next->next != NULL)
		return init_mi_tree(400, MI_MISSING_PARM_S, MI_MISSING_PARM_LEN);

	rc = str2int(&node->value, &cluster_id);
	if (rc < 0 || cluster_id < 1)
		return init_mi_tree(400, MI_SSTR(MI_BAD_PARM));

	rc = str2int(&node->next->value, &node_id);
	if (rc < 0 || node_id < 1)
		return init_mi_tree(400, MI_SSTR(MI_BAD_PARM));

	rc = str2int(&node->next->next->value, &state);
	if (rc < 0 || (state != STATE_DISABLED && state != STATE_ENABLED))
		return init_mi_tree(400, MI_SSTR(MI_BAD_PARM));

	rc = set_state(cluster_id, node_id, (int)state);
	if (rc == -1)
		return init_mi_tree(404, "Cluster id not found", 20);
	if (rc == 1)
		return init_mi_tree(404, "Node id not found", 17);

	return init_mi_tree(200, MI_SSTR(MI_OK));
}

/* lists all valid connections */
static struct mi_root * clusterer_list(struct mi_root *cmd_tree, void *param)
{
	cluster_info_t *cl;
	node_info_t *n_info;
	struct mi_root *rpl_tree = NULL;
	struct mi_node *node = NULL;
	struct mi_node *node_s = NULL;
	struct mi_attr* attr;
	str val;

	rpl_tree = init_mi_tree(200, MI_OK_S, MI_OK_LEN);
	if (!rpl_tree)
		return NULL;
	rpl_tree->node.flags |= MI_IS_ARRAY;

	lock_start_read(ref_lock);

	/* iterate through clusters */
	for (cl = *cluster_list; cl; cl = cl->next) {

		val.s = int2str(cl->cluster_id, &val.len);
		node = add_mi_node_child(&rpl_tree->node, MI_DUP_VALUE|MI_IS_ARRAY,
			MI_SSTR("Cluster"), val.s, val.len);
		if (!node) goto error;

		/* iterate through servers */
		for (n_info = cl->node_list; n_info; n_info = n_info->next) {

			val.s = int2str(n_info->node_id, &val.len);
			node_s = add_mi_node_child(node, MI_DUP_VALUE,
				MI_SSTR("Server"), val.s, val.len);
			if (!node) goto error;

			val.s = int2str(n_info->id, &val.len);
			attr = add_mi_attr(node_s, MI_DUP_VALUE,
				MI_SSTR("DB_ID"), val.s, val.len);
			if (!attr) goto error;

			attr = add_mi_attr(node_s, MI_DUP_VALUE,
				MI_SSTR("URL"), n_info->url.s, n_info->url.len);
			if (!attr) goto error;

			val.s = int2str(n_info->enabled, &val.len);
			attr = add_mi_attr(node_s, MI_DUP_VALUE,
				MI_SSTR("State"), val.s, val.len);
			if (!attr) goto error;

			val.s = int2str(n_info->no_ping_retries, &val.len);
			attr = add_mi_attr(node_s, MI_DUP_VALUE,
				MI_SSTR("No_ping_retries"), val.s, val.len);
			if (!attr) goto error;

			if (n_info->description.s)
				attr = add_mi_attr(node_s, MI_DUP_VALUE,
					MI_SSTR("Description"),
					n_info->description.s, n_info->description.len);
			else
				attr = add_mi_attr(node_s, MI_DUP_VALUE,
					MI_SSTR("Description"),
					"none", 4);
			if (!attr) goto error;
		}
	}

	lock_stop_read(ref_lock);
	return rpl_tree;

error:
	lock_stop_read(ref_lock);
	if (rpl_tree) free_mi_tree(rpl_tree);
	return NULL;
}

static void free_clusterer_node(clusterer_node_t *node)
{
	if (node->description.s)
		pkg_free(node->description.s);
	pkg_free(node);
}

static int add_clusterer_node(clusterer_node_t **cl_node_list, node_info_t *n_info)
{
	clusterer_node_t *new_node = NULL;

	new_node = pkg_malloc(sizeof *new_node);
	if (!new_node) {
		LM_ERR("no more pkg memory\n");
		goto error;
	}

	new_node->node_id = n_info->node_id;
	new_node->proto = n_info->proto;

	new_node->description.s = pkg_malloc(n_info->description.len * sizeof(char));
	if (!new_node->description.s) {
		LM_ERR("no more pkg memory\n");
		goto error;
	}
	new_node->description.len = n_info->description.len;
	memcpy(new_node->description.s, n_info->description.s, n_info->description.len);

	memcpy(&new_node->addr, &n_info->addr, sizeof(n_info->addr));
	new_node->next = NULL;

	if (*cl_node_list)
		new_node->next = *cl_node_list;

	*cl_node_list = new_node;
	return 0;
error:
	if (new_node)
		free_clusterer_node(new_node);
	return -1;
}

static void free_clusterer_nodes(clusterer_node_t *nodes)
{
	clusterer_node_t *tmp;

	while (nodes) {
		tmp = nodes;
		nodes = nodes->next;
		free_clusterer_node(tmp);
	}
}

static clusterer_node_t* get_clusterer_nodes(int cluster_id)
{
	clusterer_node_t *ret_nodes = NULL;
	node_info_t *node;

	lock_start_read(ref_lock);

	for (node = clusterer_find_nodes(cluster_id); node; node = node->next)
		if (node->enabled && node->link_state == LS_UP)
			if (add_clusterer_node(&ret_nodes, node) < 0) {
				lock_stop_read(ref_lock);
				LM_ERR("Unable to add node with id %d to the returned list of reachable nodes\n",
					node->node_id);
				free_clusterer_nodes(ret_nodes);
				return NULL;
			}

	lock_stop_read(ref_lock);

	return ret_nodes;
}

static int get_my_id(void)
{
	return current_id;
}

static inline node_info_t *clusterer_find_nodes(int cluster_id)
{
	cluster_info_t *cl;
	node_info_t *n = NULL;

	cl = *cluster_list;
	while (cl && cl->cluster_id != cluster_id)
		cl = cl->next;

	if (cl)
		n = cl->node_list;

	return n;
}

void prio_enqueue(struct node_search_info **queue_front, struct neighbour *neigh) {
	struct node_search_info *q_it;

	if (!(*queue_front)) {
		*queue_front = neigh->node->sp_info;
		return;
	}

	/* check first entry */
	if (((*queue_front)->node->priority == neigh->node->priority &&
		(*queue_front)->node->node_id > neigh->node->node_id) ||
		(*queue_front)->node->priority > neigh->node->priority) {
		neigh->node->sp_info->next = *queue_front;
		*queue_front = neigh->node->sp_info;
		return;
	}

	for (q_it = *queue_front; q_it->next; q_it = q_it->next)
		if ((q_it->next->node->priority == neigh->node->priority &&
			q_it->next->node->node_id > neigh->node->node_id) ||
			q_it->next->node->priority > neigh->node->priority) {
			neigh->node->sp_info->next = q_it->next;
			q_it->next = neigh->node->sp_info;
			return;
		}

	q_it->next = neigh->node->sp_info;
}

/* find the next hop from the path to the given destination node, according to the
 * local 'routing table', looking for paths of at least 2 links
 * @return:
 *  	> 0: next hop id
 * 		0  : no other path(node is down)
 *		< 0: error
 */
static int get_next_hop(node_info_t *dest)
{
	node_info_t *n;
	struct node_search_info *queue_front;
    struct node_search_info *root, *curr;
    struct neighbour *neigh;

    /* run BFS */
	if (dest->cluster->top_version != dest->sp_top_version) {
		dest->next_hop = NULL;

		/* init nodes search info */
		for (n = dest->cluster->node_list; n; n = n->next) {
			n->sp_info->parent = NULL;
			n->sp_info->next = NULL;
		}
		/* init root search info */
		root = dest->cluster->current_node->sp_info;
		root->parent = NULL;
		root->next = NULL;

		/* enqueue root */
		queue_front = root;

		while (queue_front) {	/* while queue not empty */
			/* dequeue */
			curr = queue_front;
			queue_front = queue_front->next;

			/* found, get back to root */
			if (curr->node == dest) {
				if (!curr->parent || !curr->parent->parent)
					return -1;
				while (curr->parent->parent)
					curr = curr->parent;
				if (curr->parent != root)
					return -1;

				dest->next_hop = curr->node;
				return dest->next_hop->node_id;
			}

			/* for each node reachable from current */
			for (neigh = curr->node->neighbour_list; neigh; neigh = neigh->next) {
				if (!neigh->node->enabled)
					continue;
				if (!neigh->node->sp_info->parent) {
					/* set parent */
					neigh->node->sp_info->parent = curr;
					/* enqueue node*/
					prio_enqueue(&queue_front, neigh);
				}
			}
		}

		dest->sp_top_version = dest->cluster->top_version;
	}

	if (dest->next_hop)
		return dest->next_hop->node_id;
	else
		return 0;
}

static inline int send_bin_msg(node_info_t *dest, int src_id) {
	str send_buffer;

	/* change the tail of packet */
	bin_alter_pop_int();
	bin_alter_pop_int();
	bin_push_int(src_id);	/* source node id */
	bin_push_int(dest->node_id); /* destination node id */

	bin_get_buffer(&send_buffer);
	if (msg_send(NULL, dest->proto, &dest->addr, 0, send_buffer.s,
				send_buffer.len, 0) != 0) {
		LM_ERR("msg_send failed\n");
		return -1;
	} else
		return 0;
}

/* @return:
 *  0 : success, message sent
 * -1 : dest disabled, message not sent
 * -2 : error, unable to send
 * -3 : dest down or probing
 */
static int clusterer_send_msg(node_info_t *dest, int src_id) {
	int retr_send = 0;
	node_info_t *chosen_dest;

	if (!dest->enabled)
		return -1;

	if (dest->proto != PROTO_BIN) {
		LM_WARN("Cannot send message to node %d, clusterer currently supports only BIN protocol\n",
			dest->node_id);
		return -2;
	}

	/* dummy values */
	bin_push_int(-1);
	bin_push_int(-1);

	do {
		if (dest->link_state == LS_UP)
			chosen_dest = dest;
		else {
			if (get_next_hop(dest) <= 0) {
				if (retr_send)
					return -2;
				else
					return -3;
			} else
				chosen_dest = dest->next_hop;
		}

		if (send_bin_msg(chosen_dest, src_id) < 0) {
			retr_send = 1;
			/* this node was supposed to be up, retry pinging */
			set_link(LS_RESTART_PINGING, chosen_dest->cluster->current_node,
				chosen_dest);
		} else
			retr_send = 0;

	} while(retr_send);

	return 0;
}

static enum clusterer_send_ret send_to(int cluster_id, int node_id)
{
	node_info_t *node;
	int rc;
	cluster_info_t *cl;

	lock_start_write(ref_lock);

	for (cl = *cluster_list; cl; cl = cl->next)
		if (cl->cluster_id == cluster_id)
			break;
	if (cl) {
		if (!cl->current_node->enabled) {
			lock_stop_write(ref_lock);
			return CLUSTERER_CURR_DISABLED;
		}
		node = cl->node_list;
	} else {
		LM_WARN("Unknown cluster, id: %d\n", cluster_id);
		lock_stop_write(ref_lock);
		return CLUSTERER_SEND_ERR;
	}

	for (; node; node = node->next)
		if (node->node_id == node_id)
			break;
	if (!node) {
		LM_ERR("Node id: %d not found in cluster\n", node_id);
		lock_stop_write(ref_lock);
		return CLUSTERER_SEND_ERR;
	}

	rc = clusterer_send_msg(node, current_id);

	lock_stop_write(ref_lock);

	switch (rc) {
	case 0:
		return CLUSTERER_SEND_SUCCES;
	case -1:
		return CLUSTERER_DEST_DISABLED;
	case -3:
		return CLUSTERER_DEST_DOWN;
	case -2:
	default :
		return CLUSTERER_SEND_ERR;
	}
}

static enum clusterer_send_ret send_all(int cluster_id)
{
	node_info_t *node;
	int rc, sent = 0, en = 0, down = 1;
	cluster_info_t *cl;

	lock_start_write(ref_lock);

	for (cl = *cluster_list; cl; cl = cl->next)
		if (cl->cluster_id == cluster_id)
			break;
	if (cl) {
		if (!cl->current_node->enabled) {
			lock_stop_write(ref_lock);
			return CLUSTERER_CURR_DISABLED;
		}
		node = cl->node_list;
	} else {
		LM_WARN("Unknown cluster, id: %d\n", cluster_id);
		lock_stop_write(ref_lock);
		return CLUSTERER_SEND_ERR;
	}

	for (; node; node = node->next) {
		rc = clusterer_send_msg(node, current_id);
		if (rc != -1)	/* at least one node is enabled */
			en = 1;
		if (rc != -3)	/* at least one node is up */
			down = 0;
		if (rc == 0)
			sent = 1;
	}

	lock_stop_write(ref_lock);

	if (!en)
		return CLUSTERER_DEST_DISABLED;
	if (down)
		return CLUSTERER_DEST_DOWN;
	if (sent)
		return CLUSTERER_SEND_SUCCES;
	else
		return CLUSTERER_SEND_ERR;
}

static void receive_clusterer_bin_packets(int packet_type, struct receive_info *ri,
											void *att)
{
	int source_id, cl_id;
	struct timeval now;
	node_info_t *node = NULL;
	cluster_info_t *cl;

	gettimeofday(&now, NULL);

	if (bin_pop_int(&cl_id)) {
		LM_ERR("Failed to get source cluster id of ping\n");
		return;
	}

	if (bin_pop_int(&source_id)) {
		LM_ERR("Failed to get source node id of ping\n");
		return;
	}

	lock_start_write(ref_lock);

	for (cl = *cluster_list; cl; cl = cl->next)
		if (cl->cluster_id == cl_id)
			break;
	if (cl) {
		if (!cl->current_node->enabled) {
			lock_stop_write(ref_lock);
			return;
		}
		node = cl->node_list;
	} else {
		LM_WARN("Received message from unknown cluster, id: %d\n", cl_id);
		lock_stop_write(ref_lock);
		return;
	}

	for (; node; node = node->next)
		if (node->node_id == source_id)
			break;
	if (!node) {
		LM_WARN("Received clusterer message from unknown source, id: %d\n", source_id);
		lock_stop_write(ref_lock);
		return;
	}

	switch (packet_type) {
	case CLUSTERER_PONG:
		LM_DBG("Received pong from node %d\n", source_id);
		node->last_pong = now;

		if (RETRIED_IS_WAITING_FOR_REPLY(node->link_state)) {
			LM_INFO("Node %d is up\n", source_id);
			set_link(LS_UP, cl->current_node, node);
		}

		break;
	case CLUSTERER_PING:
		/* reply with pong */
		if (send_heartbeat_msg(cl_id, &node->addr, CLUSTERER_PONG) < 0) {
			LM_ERR("Failed to reply to ping from node %d\n", source_id);
			if (node->link_state == LS_UP)
				set_link(LS_RESTART_PINGING, cl->current_node, node);
		} else
			LM_DBG("Replied to ping from node %d\n", source_id);

		/* if the node was down, restart pinging */
		if (node->link_state == LS_DOWN) {
			LM_INFO("Received ping from failed node, restart pinging");
			set_link(LS_RESTART_PINGING, cl->current_node, node);
		}

		break;
	default:
		LM_WARN("Invalid clusterer binary packet command from node: %d\n", source_id);
	}

	lock_stop_write(ref_lock);
}

static void bin_receive_packets(int packet_type, struct receive_info *ri, void *ptr)
{
	struct mod_registration *module;
	unsigned short port;
	int source_id, dest_id;
	char *ip;
	node_info_t *node = NULL;
	cluster_info_t *cl;

	get_su_info(&ri->src_su.s, ip, port);
	LM_DBG("received bin packet from source: %s:%hu\n", ip, port);

	/* pop the source and destination from the bin packet */
	if (bin_pop_back_int(&dest_id)) {
		LM_ERR("Failed to pop the destination node id from binary packet\n");
		return;
	}
	if (bin_pop_back_int(&source_id)) {
		LM_ERR("Failed to pop the source node id from binary packet\n");
		return;
	}

	module = (struct mod_registration *)ptr;

	lock_start_write(ref_lock);

	for (cl = *cluster_list; cl; cl = cl->next)
		if (cl->cluster_id == module->accept_cluster_id)
			break;
	if (cl) {
		if (!cl->current_node->enabled)
			return;
	} else {
		LM_WARN("Received message from unknown cluster, id: %d\n", module->accept_cluster_id);
		return;
	}
	if (module->auth_check) {
		for (node = cl->node_list; node; node = node->next)
			if (node->node_id == source_id)
				break;
		if (!node) {
			LM_WARN("Received message from unknown source, id: %d\n", source_id);
			return;
		}
	}

	if (dest_id != current_id) {
		for (node = cl->node_list; node; node = node->next)
			if (node->node_id == dest_id)
				break;
		if (!node || clusterer_send_msg(node, source_id) < 0)
			module->cb(CLUSTER_ROUTE_FAILED, packet_type, ri, source_id, dest_id);
	} else
		module->cb(CLUSTER_RECV_MSG, packet_type, ri, source_id, dest_id);

	lock_stop_write(ref_lock);
}

static int delete_neighbour(node_info_t *from_n, node_info_t *old_n)
{
	struct neighbour *neigh;

	neigh = from_n->neighbour_list;
	if (neigh->node == old_n) {
		from_n->neighbour_list = neigh->next;
		free(neigh);
		return 1;
	}
	while (neigh->next) {
		if (neigh->next->node == old_n) {
			neigh->next = neigh->next->next;
			free(neigh->next);
			return 1;
		}
		neigh = neigh->next;
	}

	return 0;
}

int add_neighbour(node_info_t *to_n, node_info_t *new_n)
{
	struct neighbour *neigh;

	neigh = to_n->neighbour_list;
	while (neigh) {
		if (neigh->node == new_n)
			return 0;
	}

	neigh = shm_malloc(sizeof *neigh);
	if (!neigh) {
		LM_ERR("No more shm mem\n");
		return -1;
	}
	neigh->node = new_n;
	neigh->next = to_n->neighbour_list;
	to_n->neighbour_list = neigh;
	return 1;
}

#define call_cbs_node_down(_node) \
	do { \
		struct cluster_mod *m_it = (_node)->cluster->modules; \
		while (m_it) { \
			m_it->reg->cb(CLUSTER_NODE_DOWN, UNDEFINED_PACKET_TYPE, NULL, \
				INVAL_NODE_ID, (_node)->node_id); \
			m_it = m_it->next; \
		} \
	} while(0)

static int set_link(clusterer_link_state new_ls, node_info_t *node_a, node_info_t *node_b)
{
	int chg = 0;

	if (node_a == node_a->cluster->current_node) {	/* link with current node's neighbours */
		if (new_ls != LS_UP && node_b->link_state == LS_UP) {
			delete_neighbour(node_a, node_b);
			node_a->cluster->top_version++;

			if (get_next_hop(node_b) <= 0)
				call_cbs_node_down(node_b);
		}
		else if (new_ls == LS_UP && node_b->link_state != LS_UP) {
			if (add_neighbour(node_a, node_b) < 0)
				return -1;
			node_a->cluster->top_version++;
		}

		node_b->link_state = new_ls;
	} else if (node_b != node_b->cluster->current_node) {
		/* for non-neighbours we only have UP or DOWN link states */
		if (new_ls == LS_DOWN) {
			chg = delete_neighbour(node_a, node_b);
			chg += delete_neighbour(node_b, node_a);
			if (chg > 0) {
				node_a->cluster->top_version++;
				if (get_next_hop(node_b) <= 0)
					call_cbs_node_down(node_b);
			}
		} else {
			chg = add_neighbour(node_a, node_b);
			if (chg < 0)
				return -1;
			chg += add_neighbour(node_b, node_a);
			if (chg < 0)
				return -1;

			if (chg > 0)
				node_a->cluster->top_version++;
		}
	}

	return 0;
}

static int cl_register_module(char *mod_name, int proto, clusterer_cb_f cb,
								int auth_check, int accept_cluster_id)
{
	struct mod_registration *new_module;

	if (accept_cluster_id < 1) {
		LM_ERR("Bad cluster_id\n");
		return -1;
	}

	new_module = shm_malloc(sizeof *new_module);
	if (!new_module) {
		LM_ERR("No more shm memory\n");
		return -1;
	}

	new_module->mod_name.len = strlen(mod_name);
	new_module->mod_name.s = mod_name;
	new_module->proto = proto;
	new_module->cb = cb;
	new_module->auth_check = auth_check;
	new_module->accept_cluster_id = accept_cluster_id;

	switch (proto) {
	case PROTO_BIN:
		bin_register_cb(mod_name, bin_receive_packets, new_module);
		break;
	default:
		LM_DBG("Clusterer currently supports only BIN protocol\n");
		shm_free(new_module);
		return -1;
	}

	new_module->next = clusterer_reg_modules;
	clusterer_reg_modules = new_module;

	LM_DBG("Registered module %s\n", mod_name);

	return 0;
}

int load_clusterer(struct clusterer_binds *binds)
{
	binds->get_nodes = get_clusterer_nodes;
	binds->set_state = set_state;
	binds->free_nodes = free_clusterer_nodes;
	binds->get_my_id = get_my_id;
	binds->send_to = send_to;
	binds->send_all = send_all;
	binds->register_module = cl_register_module;

	return 1;
}

