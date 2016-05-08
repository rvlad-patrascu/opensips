#ifndef CLUSTERER_H
#define	CLUSTERER_H

#include "../../str.h"
#include "api.h"

#define BIN_VERSION 1

#define DEFAULT_PING_INTERVAL 4
#define DEFAULT_NODE_TIMEOUT 60
#define DEFAULT_PING_TIMEOUT 1000 /* in milliseconds */

#define NO_DB_INT_VALS 5
#define NO_DB_STR_VALS 2
#define NO_DB_COLS 7

enum db_int_vals_idx {
    INT_VALS_ID_COL,
    INT_VALS_CLUSTER_ID_COL,
    INT_VALS_NODE_ID_COL,
    INT_VALS_STATE_COL,
    INT_VALS_NO_PING_RETRIES_COL
};

enum db_str_vals_idx {
    STR_VALS_URL_COL,
    STR_VALS_DESCRIPTION_COL
};

typedef enum {
    CLUSTERER_PING,
    CLUSTERER_PONG
} clusterer_msg_type;

typedef enum {
    LS_UP,
    LS_DOWN,
    LS_RETRY_SEND_FAIL,
    LS_RESTART_PINGING,
    LS_RESTARTED,
    LS_RETRYING,
} clusterer_link_state;

struct mod_registration {
   str mod_name;
   int proto;
   clusterer_cb_f cb;
   int auth_check;
   int accept_cluster_id;
   struct mod_registration *next;
};

struct cluster_mod {
    struct mod_registration *reg;
    struct cluster_mod *next;
};

struct cluster_info;

typedef struct node_info {
    int id;                             /* DB id (PK) */
    int node_id;
    int enabled;                        /* node state (enabled/disabled) */
    clusterer_link_state link_state;    /* state of the "link" with this node */
    str description;
    str url;
    int proto;
    union sockaddr_union addr;
    struct timeval last_pong;       /* last pong received from this node*/
    struct timeval last_ping;       /* last ping sent to this node*/
    int no_ping_retries;            /* maximum number of ping retries */
    int curr_no_retries;
    struct cluster_info *cluster;   /* containing cluster */
    struct node_info *next;
} node_info_t;

typedef struct cluster_info {
    int cluster_id;
    struct cluster_mod *modules;    /* modules registered for this cluster */
    node_info_t *node_list;
    node_info_t *current_node;      /* current node's info in this cluster */
    struct cluster_info *next;
} cluster_info_t;

#endif	/* CLUSTERER_H */

