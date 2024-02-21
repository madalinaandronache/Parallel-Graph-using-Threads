// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <time.h>

#include "os_graph.h"
#include "os_threadpool.h"
#include "log/log.h"
#include "utils.h"

#define NUM_THREADS		4

static int sum;
static os_graph_t *graph;
static os_threadpool_t *tp;
static pthread_mutex_t sum_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t graph_mutex =  PTHREAD_MUTEX_INITIALIZER;

// Structure to represent the graph task
typedef struct {
	int node_id;
} GraphTask;

// Add the value tot the final sum
static void add_to_sum(int value)
{
	pthread_mutex_lock(&sum_mutex);
	sum += value;
	pthread_mutex_unlock(&sum_mutex);
}

// Function to process a node in parallel
void process_node(void *task_data)
{
	GraphTask *task = (GraphTask *)task_data;

	// If the node has already been visited, skip it
	if (graph->visited[task->node_id] == DONE)
		return;

	// Mark the node as visited
	graph->visited[task->node_id] = DONE;

	// Add the current node value to the sum
	add_to_sum(graph->nodes[task->node_id]->info);

	pthread_mutex_lock(&graph_mutex);
    // Check if the neighbours can create tasks
	for (unsigned int i = 0; i < graph->nodes[task->node_id]->num_neighbours; ++i) {
		unsigned int neighbor_id = graph->nodes[task->node_id]->neighbours[i];

		// If the node has not been already visited, create its task
		if (graph->visited[neighbor_id] != DONE) {
			GraphTask *neighbor_task = malloc(sizeof(GraphTask));

			neighbor_task->node_id = neighbor_id;

			// Adding the task to the thread pool
			os_task_t *task = create_task(process_node, neighbor_task, free);

			enqueue_task(tp, task);
		}
	}
	pthread_mutex_unlock(&graph_mutex);
}

int main(int argc, char *argv[])
{
	FILE *input_file;
	GraphTask *initial_task = malloc(sizeof(GraphTask));

	if (argc != 2) {
		fprintf(stderr, "Usage: %s input_file\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	input_file = fopen(argv[1], "r");
	DIE(input_file == NULL, "fopen");

	graph = create_graph_from_file(input_file);

	tp = create_threadpool(NUM_THREADS);
	pthread_mutex_init(&sum_mutex, NULL);

	// Create the initial task for the first node
	initial_task->node_id = 0;

    // Process the first node
	process_node(initial_task);
	wait_for_completion(tp);
	destroy_threadpool(tp);

	printf("%d", sum);

	return 0;
}
