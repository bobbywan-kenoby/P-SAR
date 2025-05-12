#include "network/utils/node_id.h"
#include <pthread.h>
#include <stdatomic.h>
#include <errno.h>

#include "utils/message_type.h"
#include "submodule/protocol.h"
#include "utils/list.h"
#define ERROR_LOG
#include "utils/logger.h"
#include "network/utils/cleanup.h"
#include <stddef.h>
#include <unistd.h>

/// @brief the status of the peer to peer network for this node
enum network_status {
	STOP, // current node is stop
	START, // current node is starting
	JOINING_1, // current node try to join a peer
	JOINING_2, // current node wait response from other peers
	LEAVING_1, // current node try to leave
	LEAVING_2, // current node wait all peers confirmation
};

struct node {
	struct node_id node;
	struct list_head list;
};

static struct node *create_node(struct node_id *node_id)
{
	struct node *new_node = (struct node *)malloc(sizeof(struct node));
	if (new_node == NULL) {
		log_error("fail to create new node : %s", strerror(errno));
		return NULL;
	}
	new_node->node = *node_id;

	return new_node;
}

static struct {
	struct list_head joining_node;
	pthread_mutex_t context_mutex;
	pthread_cond_t wait_on_value_cond;
	int number_join_ack; //number ack new node receive ACK NN or LEAVE decr and ACK NN or ACK JOIN += new size
	int number_leave_ack; //number ack leave receive decr new node incr
	unsigned int number_joiners; //wait joiner node size of joining node
	atomic_int number_running_handler; //wait handler
	char state;
} context = { .context_mutex = PTHREAD_MUTEX_INITIALIZER,
	      .wait_on_value_cond = PTHREAD_COND_INITIALIZER,
	      .number_running_handler = 0,
	      .number_joiners = 0,
	      .number_join_ack = 0,
	      .number_leave_ack = 0,
	      .state = STOP };

/// @brief a helper function to take the context lock and notify main thread on leaving
static inline void handler_lock_context()
{
	context.number_running_handler++;
	pthread_mutex_lock(&context.context_mutex);
	context.number_running_handler--;
	if (context.state == STOP) {
		pthread_cond_signal(&context.wait_on_value_cond);
	}
}

/// @brief internal function to take in charge joiner node
/// @param joiner the new node
/// @return 0 on success, -1 on error
static int response_to_joiner(struct node_id *joiner)
{
	//add new node to network
	if (add_to_network(joiner) != 0) {
		log_error("fail to add %s:%d to network", joiner->host,
			  joiner->port);
		return -1;
	}

	//broadcast new node
	int size = 0;
	const struct node_id *except[2] = { joiner, NULL };
	if ((size = broadcast_message1(NETWORK_NEW_NODE, except, joiner,
				       sizeof(struct node_id))) < 0) {
		log_error("fail to inform all nodes in network");
		return -1;
	}

	//send size
	if (send_message1(NETWORK_ACK_JOIN, joiner, &size, sizeof(size_t)) !=
	    0) {
		log_error("fail to send network size to %s:%d", joiner->host,
			  joiner->port);
		return -1;
	}
	return 0;
}

/// @brief handler function for NETWORK_JOIN message
static void handle_JOIN(struct node_id *sender, void *payload)
{
	struct node_id debug = get_info();
	log_debug("node %s:%d receive JOIN from %s:%d", debug.host, debug.port,
		  sender->host, sender->port);

	handler_lock_context();
	defer_unlock_mutex(&context.context_mutex);

	// cant take join request on a leaving or a stop node
	switch (context.state) {
	case LEAVING_1:
	case LEAVING_2:
	case STOP:
		log_error("cant take joining request STOP or LEAVING");
		return;
	}

	//check if node already in the queue
	struct node *new_node;
	list_for_each_entry(new_node, &context.joining_node, list) {
		if (node_equal(&new_node->node, sender)) {
			log_warning("node already in joining queue");
			return;
		}
	}

	//add joining node to queue
	if ((new_node = create_node(sender)) == NULL) {
		log_error("fail to add node to joining list");
		return;
	}
	context.number_joiners++;
	list_add(&new_node->list, &context.joining_node);

	//response if current node already in the network
	if (context.state == START) {
		//on failure we need to remove and free the new node
		if (response_to_joiner(sender) != 0) {
			log_error("fail to take %s:%d joiner request",
				  sender->host, sender->port);
			list_del(&new_node->list);
			free(new_node);
		}
	}
}

/// @brief handler function for NETWORK_ACK_JOIN message
static void handle_ACK_JOIN(struct node_id *sender, void *payload)
{
	struct node_id debug = get_info();
	log_debug("node %s:%d receive ACK JOIN from %s:%d", debug.host,
		  debug.port, sender->host, sender->port);

	handler_lock_context();
	defer_unlock_mutex(&context.context_mutex);

	if (context.state != JOINING_1) {
		log_error("not joining network");
		return;
	}

	// phase 2 main thread wait all ack from network
	context.state = JOINING_2;
	size_t network_size = *(size_t *)payload;
	context.number_join_ack += network_size;
	pthread_cond_signal(&context.wait_on_value_cond);
}

/// @brief handler function for NETWORK_NEW_NODE message
static void handle_NEW_NODE(struct node_id *sender, void *payload)
{
	struct node_id debug = get_info();
	log_debug("node %s:%d receive NEW NODE from %s:%d", debug.host,
		  debug.port, sender->host, sender->port);

	handler_lock_context();
	defer_unlock_mutex(&context.context_mutex);

	if (context.state == STOP) {
		log_error("node is STOP");
		return;
	}

	struct node_id *joining_node =
		(struct node_id *)payload; // need serialization

	//add new node to network
	if (add_to_network(joining_node) != 0)
		log_error("fail to add %s:%d to network", joining_node->host,
			  joining_node->port);

	//send leave asking to new node and nothing to do
	if (context.state == LEAVING_2) {
		if (send_message1(NETWORK_LEAVE, joining_node, NULL, 0) != 0) {
			log_error("fail to contact joining node %s:%d",
				  joining_node->host, joining_node->port);
		} else {
			context.number_leave_ack++;
		}
		return;
	}

	//create joiners list to send end with EMPTY_NODE
	size_t payload_size =
		sizeof(struct node_id) * (context.number_joiners + 1);
	struct node_id *joiner_to_send = malloc(payload_size);
	if (joiner_to_send == NULL) {
		log_error("fail to alloc payload: %s", strerror(errno));
		return;
	}
	int njoiner = 0;
	struct node *cur;
	list_for_each_entry(cur, &context.joining_node, list) {
		*(joiner_to_send + njoiner) = cur->node;
		njoiner++;
	}
	*(joiner_to_send + njoiner) = EMPTY_NODE;

	//send ACK_NEW_NODE with all joiners
	if (send_message1(NETWORK_ACK_NEW_NODE, joining_node, joiner_to_send,
			  payload_size) != 0) {
		log_error("fail to contact joining node %s:%d",
			  joining_node->host, joining_node->port);
	}

	//forward to all joiner node that a new node has join
	list_for_each_entry(cur, &context.joining_node, list) {
		if (send_message1(NETWORK_FORW_NEW_NODE, &cur->node,
				  joining_node, sizeof(struct node_id)) != 0) {
			log_error("fail to inform node %s:%d", cur->node.host,
				  cur->node.port);
		}
	}

	free(joiner_to_send);
}

static void handle_FORW_NEW_NODE(struct node_id *sender, void *payload)
{
	handler_lock_context();
	defer_unlock_mutex(&context.context_mutex);

	if (context.state != JOINING_1 && context.state != JOINING_2) {
		log_warning("unexpected receive");
	}

	struct node_id *joining_node = (struct node_id *)payload;

	//add new node to network
	if (add_to_network(joining_node) != 0)
		log_error("fail to add %s:%d to network", joining_node->host,
			  joining_node->port);
}

static void handle_ACK_NEW_NODE(struct node_id *sender, void *payload)
{
	struct node_id debug = get_info();
	log_debug("node %s:%d receive ACK NEW NODE from %s:%d", debug.host,
		  debug.port, sender->host, sender->port);

	handler_lock_context();
	defer_unlock_mutex(&context.context_mutex);

	if (context.state != JOINING_1 && context.state != JOINING_2) {
		log_error("not joining network");
		return;
	}

	struct node_id *nodes_list = payload;
	while (node_isempty(nodes_list) == false) {
		add_to_network(nodes_list);
		nodes_list++;
	}

	//signal main thread
	context.number_join_ack--;
	pthread_cond_signal(&context.wait_on_value_cond);

	//add sender to network
	add_to_network(sender);
}

static void handle_JOIN_SUCCESS(struct node_id *sender, void *payload)
{
	struct node_id debug = get_info();

	handler_lock_context();
	defer_unlock_mutex(&context.context_mutex);

	switch (context.state) {
	case JOINING_1:
	case JOINING_2:
	case STOP:
	case LEAVING_2:
		log_error("unexpected JOIN SUCCESS or wrong state");
		return;
	}

	//remove joining node
	struct node *cur, *tmp;
	list_for_each_entry_safe(cur, tmp, &context.joining_node, list) {
		if (node_equal(&cur->node, sender)) {
			list_del(&cur->list);
			free(cur);
			context.number_joiners--;
			pthread_cond_signal(&context.wait_on_value_cond);
			break;
		}
	}

	log_debug("node %s:%d receive JOIN SUCCESS from %s:%d remaining=%d",
		  debug.host, debug.port, sender->host, sender->port,
		  context.number_joiners);
	return;
}

static void handle_LEAVE(struct node_id *sender, void *payload)
{
	struct node_id debug = get_info();
	log_debug("node %s:%d receive LEAVE from %s:%d", debug.host, debug.port,
		  sender->host, sender->port);

	handler_lock_context();
	defer_unlock_mutex(&context.context_mutex);

	if (context.state == STOP) {
		log_error("node is STOP");
		return;
	}

	// confirm leave to leaver
	if (send_message1(NETWORK_ACK_LEAVE, sender, NULL, 0) != 0) {
		log_error("fail to ACK LEAVE");
	}

	// if node not in network and state STARTING update ack number
	if (remove_from_network(sender) != 0) {
		if (context.state == JOINING_2) {
			context.number_join_ack--;
			pthread_cond_signal(&context.wait_on_value_cond);
		} else {
			log_error("node %s:%d was not in the network",
				  sender->host, sender->port);
		}
	}
}

static void handle_ACK_LEAVE(struct node_id *sender, void *payload)
{
	struct node_id debug = get_info();

	handler_lock_context();
	defer_unlock_mutex(&context.context_mutex);

	if (context.state != LEAVING_2) {
		log_error("node is not in LEAVING_2");
		return;
	}

	remove_from_network(sender);

	context.number_leave_ack--;

	log_debug("node %s:%d receive ACK LEAVE from %s:%d remaining=%d",
		  debug.host, debug.port, sender->host, sender->port,
		  context.number_leave_ack);

	pthread_cond_signal(&context.wait_on_value_cond);
}

/// @brief function to join a network
/// @details This function initialize a network and try to join a network
/// @param me information of current node
/// @param father information of the node in the network to join
/// @return 0 on success or -1 on error
int join_network(const struct node_id *me, const struct node_id *father)
{
	pthread_mutex_lock(&context.context_mutex);
	defer_unlock_mutex(&context.context_mutex);

	if (context.state != STOP) {
		log_error("network is not STOP");
		return -1;
	}

	//init network
	if (init_network(me, NUMBER_OF_MSG_TYPE) != 0) {
		log_error("fail to init network");
		return -1;
	}

	//set context (need to clean it on error ??)
	INIT_LIST_HEAD(&context.joining_node);
	context.state = JOINING_1;
	context.number_running_handler = 0;
	context.number_joiners = 0;
	context.number_join_ack = 0;
	context.number_leave_ack = 0;

	//set handler
	int err = 0;
	err += add_net_handler(NETWORK_LEAVE, handle_LEAVE);
	err += add_net_handler(NETWORK_JOIN, handle_JOIN);
	err += add_net_handler(NETWORK_NEW_NODE, handle_NEW_NODE);
	err += add_net_handler(NETWORK_JOIN_SUCCESS, handle_JOIN_SUCCESS);
	if (err != 0) {
		log_error("fail to add handler");
		return -1;
	}

	//join father
	if (father != NULL) {
		//set joining handler
		err += add_net_handler(NETWORK_ACK_JOIN, handle_ACK_JOIN);
		err += add_net_handler(NETWORK_ACK_NEW_NODE,
				       handle_ACK_NEW_NODE);
		err += add_net_handler(NETWORK_FORW_NEW_NODE,
				       handle_FORW_NEW_NODE);
		if (err != 0) {
			log_error("fail to add joining handler");
			return -1;
		}

		//send JOIN to father
		if (add_to_network(father) != 0) {
			log_error("fail to connect to %s:%d", father->host,
				  father->port);
			context.state = STOP;
			return -1;
		}

		if (send_message1(NETWORK_JOIN, father, (void *)me,
				  sizeof(struct node_id)) != 0) {
			log_error("fail to join %s:%d network", father->host,
				  father->port);
			return -1;
		}

		//wait ack from network
		while (context.state != JOINING_2 ||
		       context.number_join_ack != 0) {
			//if STOP cleanup ??
			pthread_cond_wait(&context.wait_on_value_cond,
					  &context.context_mutex);
		}

		//unset joining handler
		err += add_net_handler(NETWORK_ACK_JOIN, NULL);
		err += add_net_handler(NETWORK_ACK_NEW_NODE, NULL);
		err += add_net_handler(NETWORK_FORW_NEW_NODE, NULL);
		if (err != 0) {
			log_warning("fail to remove joining handler");
		}

		//send success
		if (send_message1(NETWORK_JOIN_SUCCESS, father, NULL, 0) != 0) {
			log_warning("fail inform %s:%d of success",
				    father->host, father->port);
		}
	}

	context.state = START;

	//catch up joining request
	struct node *cur, *tmp;
	list_for_each_entry_safe(cur, tmp, &context.joining_node, list) {
		if (response_to_joiner(&cur->node) != 0) {
			log_error("fail to response to joiniing node %s:%d",
				  cur->node.host, cur->node.port);
			list_del(&cur->list);
			free(cur);
		}
	}

	return 0;
}

/// @brief function to leave the network
/// @return 0 on success or -1 on failure
int leave_network(void)
{
	pthread_mutex_lock(&context.context_mutex);
	defer_unlock_mutex(&context.context_mutex);

	if (context.state != START) {
		log_error("network is not START");
		return -1;
	}

	context.state = LEAVING_1;

	log_debug("wait joiner remaining=%d", context.number_joiners);

	//wait all joiner to join
	while (context.number_joiners != 0) {
		pthread_cond_wait(&context.wait_on_value_cond,
				  &context.context_mutex);
	}

	log_debug("phase 2");

	//set leaving handler
	context.state = LEAVING_2;
	add_net_handler(NETWORK_ACK_LEAVE, handle_ACK_LEAVE);

	//send leave request
	context.number_leave_ack =
		broadcast_message1(NETWORK_LEAVE, NULL, NULL, 0);
	if (context.number_leave_ack == -1) {
		log_warning("fail to broadcast network may be corrupt");
	} else {
		//wait leave confirm
		while (context.number_leave_ack != 0) {
			pthread_cond_wait(&context.wait_on_value_cond,
					  &context.context_mutex);
		}
	}

	log_debug("node receive leaving confirmation");

	//unset handler
	int err = 0;
	err += add_net_handler(NETWORK_ACK_LEAVE, NULL);

	err += add_net_handler(NETWORK_JOIN, NULL);
	err += add_net_handler(NETWORK_NEW_NODE, NULL);
	err += add_net_handler(NETWORK_JOIN_SUCCESS, NULL);
	err += add_net_handler(NETWORK_LEAVE, NULL);
	if (err != 0) {
		log_warning("fail to remove handler");
	}

	context.state = STOP;
	while (context.number_running_handler != 0) {
		pthread_cond_wait(&context.wait_on_value_cond,
				  &context.context_mutex);
	}

	//reset context
	context.number_running_handler = 0;
	context.number_joiners = 0;

	//exit network
	if (exit_network() != 0) {
		log_error("fail to exit network");
		return -1;
	}

	return 0;
}