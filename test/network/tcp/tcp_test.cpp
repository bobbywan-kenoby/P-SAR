#include "network/utils/node_id.h"
#include <gtest/gtest.h>
#include <thread>
#include <algorithm>
#include "../test/utils.h"

extern "C" {
#include "comm/comm.h"
#include "network/submodule/protocol.h"
#include "utils/logger.h"
}

TEST(TCP, CorrectOrder)
{
	struct node_id info = {
		.host = "127.0.0.1",
		.port = 6000,
	};
	ASSERT_EQ(init_comm(), 0);
	ASSERT_EQ(init_network(&info, 1), 0);
	ASSERT_EQ(exit_network(), 0);
	ASSERT_EQ(exit_comm(), 0);
}

TEST(TCP, InitNetworkBeforeComm)
{
	struct node_id info = {
		.host = "127.0.0.1",
		.port = 6000,
	};
	EXPECT_NE(init_network(&info, 1), 0);
}

TEST(TCP, ExitNetworkBeforeInit)
{
	struct node_id info = {
		.host = "127.0.0.1",
		.port = 6000,
	};
	ASSERT_EQ(init_comm(), 0);
	EXPECT_NE(exit_network(), 0);
	ASSERT_EQ(exit_comm(), 0);
}

TEST(TCP, ExitNetworkThreadSafe)
{
	struct node_id info = {
		.host = "127.0.0.1",
		.port = 6000,
	};
	ASSERT_EQ(init_comm(), 0);
	ASSERT_EQ(init_network(&info, 1), 0);

	const int kThreads = 5;
	std::vector<std::thread> threads;
	std::vector<int> results(kThreads);

	for (int i = 0; i < kThreads; ++i) {
		threads.emplace_back(
			[&results, i]() { results[i] = exit_network(); });
	}

	for (auto &t : threads) {
		t.join();
	}

	int success = 0, failure = 0;
	for (int i = 0; i < kThreads; ++i) {
		if (results[i] == 0)
			++success;
		else
			++failure;
	}
	EXPECT_EQ(success, 1);
	EXPECT_EQ(failure, kThreads - 1);

	ASSERT_EQ(exit_comm(), 0);
}

TEST(TCP, AddToNetwork)
{
	std::vector<struct node_id> list;

	for (int i = 0; i < 6; ++i) {
		struct node_id node {
			.host = "127.0.0.1", .port = 6000 + i,
		};
		list.push_back(node);
	}

	struct barrier *start = barrier_init(list.size());
	struct barrier *end = barrier_init(list.size());

	for (auto node : list) {
		pid_t pid = fork();
		int cur_barrier = 0;
		if (pid == 0) {
			EXPECT_TRUE_OR_EXIT(init_comm() == 0);
			EXPECT_TRUE_OR_EXIT(init_network(&node, 0) == 0);

			barrier_wait(start);

			log_info("test start");

			for (auto n : list) {
				if (!node_equal(&n, &node)) {
					EXPECT_TRUE_OR_EXIT(
						add_to_network(&n) == 0);
				}
			}
			unsigned int size;
			struct node_id *nodes = get_network(&size);
			EXPECT_TRUE_OR_EXIT(nodes != NULL);

			// log_info("%d == %zu", size, list.size());
			for (int v = 0; v < list.size(); v++) {
				bool find = false;
				for (int j = 0; j < size; j++) {
					if (node_equal(&nodes[j], &list[v])) {
						find = true;
						break;
					}
				}
				EXPECT_TRUE_OR_EXIT(find);
			}
			free(nodes);

			log_info("node %s:%d reach", node.host, node.port);

			EXPECT_TRUE_OR_EXIT(size == list.size());

			barrier_wait(end);

			log_info("test end");

			EXPECT_TRUE_OR_EXIT(exit_network() == 0);
			EXPECT_TRUE_OR_EXIT(exit_comm() == 0);

			log_info("network clean");

			_exit(0);
		}
	}

	for (auto &n : list) {
		wait(NULL);
	}
}

static bool check_network(std::vector<struct node_id> list,
			  struct node_id *network, int size)
{
	for (int v = 0; v < list.size(); v++) {
		bool find = false;
		for (int j = 0; j < size; j++) {
			if (node_equal(&network[j], &list[v])) {
				find = true;
				break;
			}
		}
		if (find == false) {
			return false;
		}
	}
	return true;
}

TEST(TCP, RemoveFromNetwork)
{
	std::vector<struct node_id> list;

	for (int i = 0; i < 6; ++i) {
		struct node_id node {
			.host = "127.0.0.1", .port = 6000 + i,
		};
		list.push_back(node);
	}

	struct barrier *start = barrier_init(list.size());
	struct barrier *test = barrier_init(list.size());
	struct barrier *end = barrier_init(list.size());

	for (int i = 0; i < list.size(); i++) {
		pid_t pid = fork();
		int cur_barrier = 0;
		if (pid == 0) {
			EXPECT_TRUE_OR_EXIT(init_comm() == 0);
			EXPECT_TRUE_OR_EXIT(init_network(&list[i], 0) == 0);

			barrier_wait(start);

			log_info("start");

			//add all node to network
			for (auto n : list) {
				if (!node_equal(&n, &list[i])) {
					EXPECT_TRUE_OR_EXIT(
						add_to_network(&n) == 0);
				}
			}

			//check network
			unsigned int size;
			struct node_id *nodes = get_network(&size);
			EXPECT_TRUE_OR_EXIT(nodes != NULL);
			EXPECT_TRUE_OR_EXIT(size == list.size());
			EXPECT_TRUE_OR_EXIT(check_network(list, nodes, size));
			free(nodes);

			barrier_wait(test);

			//try to remove next node from network
			struct node_id target = list[(i + 1) % list.size()];
			EXPECT_TRUE_OR_EXIT(remove_from_network(&target) == 0);
			list.erase(
				std::remove_if(
					list.begin(), list.end(),
					// Lambda qui renvoie true si le nom correspond
					[&](const struct node_id t) {
						return node_equal(&target, &t);
					}),
				list.end());

			//another check network
			nodes = get_network(&size);
			EXPECT_TRUE_OR_EXIT(nodes != NULL);
			EXPECT_TRUE_OR_EXIT(size == list.size());
			EXPECT_TRUE_OR_EXIT(check_network(list, nodes, size));
			free(nodes);

			barrier_wait(end);

			log_info("test end");

			EXPECT_TRUE_OR_EXIT(exit_network() == 0);
			EXPECT_TRUE_OR_EXIT(exit_comm() == 0);

			log_info("network clean");

			_exit(0);
		}
	}

	for (auto &n : list) {
		wait(NULL);
	}
}

pthread_cond_t waithandler = PTHREAD_COND_INITIALIZER;
pthread_mutex_t handlermutex = PTHREAD_MUTEX_INITIALIZER;
bool ready = false;
static void handler_test(struct node_id *sender, void *payload)
{
	log_info("message handle");
	EXPECT_TRUE_OR_EXIT(*(int *)payload == 1);
	ready = true;
	pthread_cond_broadcast(&waithandler);
}

TEST(TCP, SendToNode)
{
	std::vector<struct node_id> list;

	for (int i = 0; i < 5; ++i) {
		struct node_id node {
			.host = "127.0.0.1", .port = 6000 + i,
		};
		list.push_back(node);
	}

	struct barrier *start = barrier_init(list.size());
	struct barrier *end = barrier_init(list.size());

	for (int i = 0; i < list.size(); ++i) {
		pid_t pid = fork();
		int cur_barrier = 0;
		if (pid == 0) {
			EXPECT_TRUE_OR_EXIT(init_comm() == 0);
			EXPECT_TRUE_OR_EXIT(init_network(&list[i], 2) == 0);
			EXPECT_TRUE_OR_EXIT(add_net_handler(0, handler_test) ==
					    0);

			barrier_wait(start);

			log_info("test start");

			EXPECT_TRUE_OR_EXIT(
				add_to_network(&list[i % list.size()]) == 0);

			int payload = 1;
			EXPECT_TRUE_OR_EXIT(
				send_message1(0, &list[i % list.size()],
					      &payload, sizeof(payload)) == 0);

			//wait handler
			pthread_mutex_lock(&handlermutex);
			while (ready == false) {
				pthread_cond_wait(&waithandler, &handlermutex);
			}
			pthread_mutex_unlock(&handlermutex);

			barrier_wait(end);

			EXPECT_TRUE_OR_EXIT(exit_network() == 0);
			EXPECT_TRUE_OR_EXIT(exit_comm() == 0);

			_exit(0);
		}
	}

	for (auto &n : list) {
		wait(NULL);
	}
}

int verif = 0;
int count = 0;
static void handlerFIFO(struct node_id *sender, void *payload)
{
	log_info("handler receive %d", *(int *)payload);
	usleep(500);
	count++;
	verif = *(int *)payload;
	log_info("handler finish");
	pthread_cond_broadcast(&waithandler);
}

//maintenir FIFO
TEST(TCP, TESTFIFO)
{
	struct node_id parent = {
		.host = "127.0.0.1",
		.port = 6000,
	};

	struct node_id child = {
		.host = "127.0.0.1",
		.port = 6001,
	};

	struct barrier *start = barrier_init(2);
	struct barrier *end = barrier_init(2);

	ready = false;
	const int msg_send = 10;

	pid_t pid = fork();
	if (pid == 0) {
		EXPECT_TRUE_OR_EXIT(init_comm() == 0);
		EXPECT_TRUE_OR_EXIT(init_network(&child, 1) == 0);

		//wait before test
		barrier_wait(start);

		EXPECT_TRUE_OR_EXIT(add_to_network(&parent) == 0);

		for (int i = 0; i < msg_send; i++) {
			// log_info("send %d", i);
			EXPECT_TRUE_OR_EXIT(
				send_message1(0, &parent, &i, sizeof(i)) == 0);
		}

		//wait before end
		barrier_wait(end);

		EXPECT_TRUE_OR_EXIT(exit_network() == 0);
		EXPECT_TRUE_OR_EXIT(exit_comm() == 0);

		_exit(0);
	} else {
		ASSERT_EQ(init_comm(), 0);
		ASSERT_EQ(init_network(&parent, 1), 0);

		ASSERT_EQ(add_net_handler(0, handlerFIFO), 0);

		//wait before test
		barrier_wait(start);

		//wait handler
		pthread_mutex_lock(&handlermutex);
		while (count != msg_send) {
			pthread_cond_wait(&waithandler, &handlermutex);
		}
		pthread_mutex_unlock(&handlermutex);

		ASSERT_EQ(verif, msg_send - 1);

		//wait before end
		barrier_wait(end);

		ASSERT_EQ(exit_network(), 0);
		ASSERT_EQ(exit_comm(), 0);
	}

	wait(NULL);
}

pthread_cond_t bc_cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t bc_mutex = PTHREAD_MUTEX_INITIALIZER;
int bc_count = 0;

// Handler commun pour tous les nœuds
static void bc_handler(struct node_id *sender, void *payload)
{
	int received = *(int *)payload;
	EXPECT_TRUE_OR_EXIT(received == 42);
	pthread_mutex_lock(&bc_mutex);
	++bc_count;
	pthread_cond_signal(&bc_cond);
	pthread_mutex_unlock(&bc_mutex);
}

TEST(TCP, BroadcastMessage)
{
	const int K = 4; // nombre de récepteurs
	std::vector<struct node_id> nodes;
	// construire K+1 noeuds (un émetteur + K récepteurs)
	for (int i = 0; i <= K; ++i) {
		struct node_id n = { .host = "127.0.0.1", .port = 8000 + i };
		nodes.push_back(n);
	}

	struct barrier *start = barrier_init(K + 1);
	struct barrier *test = barrier_init(K + 1);
	struct barrier *end = barrier_init(K + 1);

	// Lancer K+1 processus
	for (int i = 0; i <= K; ++i) {
		pid_t pid = fork();
		if (pid == 0) {
			// Processus enfant
			EXPECT_TRUE_OR_EXIT(init_comm() == 0);
			EXPECT_TRUE_OR_EXIT(init_network(&nodes[i], 1) == 0);
			EXPECT_TRUE_OR_EXIT(add_net_handler(0, bc_handler) ==
					    0);

			// Tous attendent avant de s'ajouter mutuellement
			barrier_wait(start);

			// Chaque noeud (sauf l’émetteur) s’ajoute au réseau
			for (int j = 0; j <= K; ++j) {
				if (j != i) {
					EXPECT_TRUE_OR_EXIT(
						add_to_network(&nodes[j]) == 0);
				}
			}

			barrier_wait(test); // synchronisation avant broadcast

			if (i == 0) {
				// Seul noeud 0 émet le broadcast
				int msg = 42;
				// except = { NULL } pour ne pas exclure
				EXPECT_TRUE_OR_EXIT(
					broadcast_message1(0, nullptr, &msg,
							   sizeof(msg)) == K);
			} else {
				// les autres attendent la réception
				pthread_mutex_lock(&bc_mutex);
				while (bc_count == 0) {
					pthread_cond_wait(&bc_cond, &bc_mutex);
				}
				pthread_mutex_unlock(&bc_mutex);
			}

			barrier_wait(end); // synchronisation avant de quitter

			EXPECT_TRUE_OR_EXIT(exit_network() == 0);
			EXPECT_TRUE_OR_EXIT(exit_comm() == 0);
			_exit(0);
		}
	}

	// Parent attend la fin de tous les enfants
	for (int i = 0; i <= K; ++i) {
		wait(nullptr);
	}
}

pthread_cond_t bc_cond2 = PTHREAD_COND_INITIALIZER;
pthread_mutex_t bc_mutex2 = PTHREAD_MUTEX_INITIALIZER;
int bc_count2 = 0;

// Handler de réception pour tous les nœuds
static void bc_handler2(struct node_id *sender, void *payload)
{
	int received = *(int *)payload;
	EXPECT_TRUE_OR_EXIT(received == 123);
	pthread_mutex_lock(&bc_mutex2);
	++bc_count2;
	pthread_cond_signal(&bc_cond2);
	pthread_mutex_unlock(&bc_mutex2);
}

TEST(TCP, BroadcastMessageWithExcept)
{
	const int K = 4; // nombre de récepteurs
	std::vector<struct node_id> nodes;
	// construire K+1 nœuds (un émetteur + K récepteurs)
	for (int i = 0; i <= K; ++i) {
		struct node_id n = { .host = "127.0.0.1", .port = 9000 + i };
		nodes.push_back(n);
	}

	// Création des barriers pour synchroniser K+1 processus
	struct barrier *start = barrier_init(K + 1);
	struct barrier *test = barrier_init(K + 1);
	struct barrier *end = barrier_init(K + 1);

	for (int i = 0; i <= K; ++i) {
		pid_t pid = fork();
		if (pid == 0) {
			// Initialisation
			EXPECT_TRUE_OR_EXIT(init_comm() == 0);
			EXPECT_TRUE_OR_EXIT(init_network(&nodes[i], 1) == 0);
			EXPECT_TRUE_OR_EXIT(add_net_handler(0, bc_handler2) ==
					    0);

			// Tous les nœuds attendent d'être prêts
			barrier_wait(start);

			// Chacun ajoute tous les pairs (pour que l'info de connexion soit en place)
			for (int j = 0; j <= K; ++j) {
				if (j != i) {
					EXPECT_TRUE_OR_EXIT(
						add_to_network(&nodes[j]) == 0);
				}
			}

			barrier_wait(
				test); // Synchronisation avant le broadcast

			if (i == 0) {
				// Noeud 0 émetteur : on exclut volontairement nodes[2]
				int msg = 123;
				const struct node_id *except_list[] = {
					&nodes[2], nullptr
				};
				// On attend K-1 envois (K récepteurs moins 1 exclu)
				EXPECT_TRUE_OR_EXIT(
					broadcast_message1(0, except_list, &msg,
							   sizeof(msg)) ==
					K - 1);
			} else if (i != 2) {
				// tous les autres doivent recevoir exactement 1 message
				pthread_mutex_lock(&bc_mutex2);
				while (bc_count2 == 0) {
					pthread_cond_wait(&bc_cond2,
							  &bc_mutex2);
				}
				EXPECT_TRUE_OR_EXIT(bc_count2 == 1);
				pthread_mutex_unlock(&bc_mutex2);
			}

			barrier_wait(end); // Synchronisation avant la fin

			// node[2] ne doit pas avoir recu le broadcast
			if (i == 2) {
				EXPECT_TRUE_OR_EXIT(bc_count2 == 0);
			}

			// Teardown
			EXPECT_TRUE_OR_EXIT(exit_network() == 0);
			EXPECT_TRUE_OR_EXIT(exit_comm() == 0);
			_exit(0);
		}
	}

	// Parent attend la fin de tous les enfants
	for (int i = 0; i <= K; ++i) {
		wait(nullptr);
	}
}