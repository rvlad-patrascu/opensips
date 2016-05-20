#ifndef API_H
#define	API_H

#include "../../str.h"

#define UNDEFINED_PACKET_TYPE -1
#define INVAL_NODE_ID -1

enum cl_node_state {
    STATE_DISABLED,
	STATE_ENABLED
};

typedef struct clusterer_node {
    int node_id;
    int proto;
    str description;
    union sockaddr_union addr;
    struct clusterer_node *next;
} clusterer_node_t;

enum clusterer_send_ret {
    CLUSTERER_SEND_SUCCES = 0,
    CLUSTERER_CURR_DISABLED = 1,  /* current node disabled */
    CLUSTERER_DEST_DISABLED = -1, /* destination node(s) disabled */
    CLUSTERER_DEST_DOWN = -2,     /* destination node(s) down or probing */
    CLUSTERER_SEND_ERR = -3,      /* error */
};

enum clusterer_event {
    /* node with id provided in the @dest_id param of clusterer_cb_f is unreachable */
    CLUSTER_NODE_DOWN,
    /* failed to route received message (source and destination nodes
     * provided in clusterer_cb_f params) */
    CLUSTER_ROUTE_FAILED,
    /* received message for current node */
    CLUSTER_RECV_MSG
};

/* returns the list of reachable nodes in the cluster */
typedef clusterer_node_t* (*get_nodes_f)(int cluster_id);

/* free the list returned by the get_nodes_f function */
typedef void (*free_nodes_f)(clusterer_node_t *list);

/* sets the state (enabled or disabled) of a node from a cluster */
typedef int (*set_state_f)(int cluster_id, int node_id, enum cl_node_state state);

/* get the node id of the current node */
typedef int (*get_my_id_f)(void);

/* send message to specified node in the cluster */
typedef enum clusterer_send_ret (*send_to_f)(int cluster_id, int node_id);

/* send message to all nodes in the cluster */
typedef enum clusterer_send_ret (*send_all_f)(int cluster_id);

/*
 * This function will be called for every binary packet received or
 * to signal certain cluster events.
 */
typedef void (*clusterer_cb_f)(enum clusterer_event ev, int packet_type,
                                struct receive_info *ri, int src_id, int dest_id);

/* Register module to clusterer */
typedef int (*register_module_f)(char *mod_name, int proto,  clusterer_cb_f cb,
                                    int auth_check, int accept_cluster_id);

struct clusterer_binds {
    get_nodes_f get_nodes;
    free_nodes_f free_nodes;
    set_state_f set_state;
    get_my_id_f get_my_id;
    send_to_f send_to;
    send_all_f send_all;
    register_module_f register_module;
};

typedef int (*load_clusterer_f)(struct clusterer_binds *binds);

int load_clusterer(struct clusterer_binds *binds);

static inline int load_clusterer_api(struct clusterer_binds *binds) {
    load_clusterer_f load_clusterer;

    /* import the DLG auto-loading function */
    if (!(load_clusterer = (load_clusterer_f) find_export("load_clusterer", 0, 0)))
        return -1;

    /* let the auto-loading function load all DLG stuff */
    if (load_clusterer(binds) == -1)
        return -1;

    return 0;
}

#endif	/* API_H */

