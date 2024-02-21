// SPDX-License-Identifier: BSD-3-Clause

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>

#include "os_threadpool.h"
#include "log/log.h"
#include "utils.h"

static unsigned int waiting_threads; // the number of waiting threads
static int start; // is 0 if no work has been done yet

/* Create a task that would be executed by a thread. */
os_task_t *create_task(void (*action)(void *), void *arg, void (*destroy_arg)(void *))
{
	os_task_t *t;

	t = malloc(sizeof(*t));
	DIE(t == NULL, "malloc");

	t->action = action;		// the function
	t->argument = arg;		// arguments for the function
	t->destroy_arg = destroy_arg;	// destroy argument function

	return t;
}

/* Destroy task. */
void destroy_task(os_task_t *t)
{
	if (t->destroy_arg != NULL)
		t->destroy_arg(t->argument);
	free(t);
}

/*
 * Check if queue is empty.
 * This function should be called in a synchronized manner.
 */
static int queue_is_empty(os_threadpool_t *tp)
{
	return list_empty(&tp->head);
}

/* Put a new task to threadpool task queue. */
void enqueue_task(os_threadpool_t *tp, os_task_t *t)
{
	start = 1;
	assert(tp != NULL);
	assert(t != NULL);

	// Enqueue task to the shared task queue
	pthread_mutex_lock(&tp->mutex);
	list_add_tail(&tp->head, &t->list);
	pthread_cond_signal(&tp->condition);
	pthread_mutex_unlock(&tp->mutex);
}


/*
 * Get a task from threadpool task queue.
 * Block if no task is available.
 * Return NULL if work is complete, i.e. no task will become available,
 * i.e. all threads are going to block.
 */

os_task_t *dequeue_task(os_threadpool_t *tp)
{
	pthread_mutex_lock(&tp->mutex);
	os_task_t *t = NULL;

	waiting_threads++;
	// A task is dequeued if we can
	if (!queue_is_empty(tp) && tp->active == 1) {
		t = list_entry(tp->head.next, os_task_t, list);
		list_del(tp->head.next);
		waiting_threads--;
	}

	pthread_mutex_unlock(&tp->mutex);
	return t;
}

/* Loop function for threads */
static void *thread_loop_function(void *arg)
{
	os_threadpool_t *tp = (os_threadpool_t *)arg;
	os_task_t *task = NULL;

	while (tp->active == 1) {
		task = dequeue_task(tp);

		if (task == NULL) {
			pthread_mutex_lock(&tp->mutex);
			// If we are at the last thread we want to stop the work
			if (waiting_threads == tp->num_threads) {
				tp->active = 0;
				pthread_cond_broadcast(&tp->condition);
			} else if (queue_is_empty(tp) && start == 1) {
				// We are waiting for tasks
				pthread_cond_wait(&tp->condition, &tp->mutex);
			}
			pthread_mutex_unlock(&tp->mutex);
			break;
		}

		task->action(task->argument);
		destroy_task(task);
	}

	return NULL;
}

/* Wait completion of all threads. This is to be called by the main thread. */
void wait_for_completion(os_threadpool_t *tp)
{
	for (unsigned int i = 0; i < tp->num_threads; i++)
		pthread_join(tp->threads[i], NULL);
}


/* Create a new threadpool. */
os_threadpool_t *create_threadpool(unsigned int num_threads)
{
	os_threadpool_t *tp = NULL;
	int rc;

	tp = malloc(sizeof(*tp));
	DIE(tp == NULL, "malloc");

	list_init(&tp->head);

	// Initialize synchronization data

	pthread_mutex_init(&tp->mutex, NULL);
	pthread_cond_init(&tp->condition, NULL);
	tp->active = 1;
	waiting_threads = 0;
	tp->num_threads = num_threads;
	tp->threads = malloc(num_threads * sizeof(*tp->threads));
	DIE(tp->threads == NULL, "malloc");
	for (unsigned int i = 0; i < num_threads; ++i) {
		rc = pthread_create(&tp->threads[i], NULL, &thread_loop_function, (void *) tp);
		DIE(rc < 0, "pthread_create");
	}

	return tp;
}

/* Destroy a threadpool. Assume all threads have been joined. */
void destroy_threadpool(os_threadpool_t *tp)
{
	os_list_node_t *n, *p;

	// Cleanup synchronization data

	tp->active = 0;
	pthread_mutex_destroy(&tp->mutex);
	pthread_cond_destroy(&tp->condition);
	waiting_threads = 0;

	list_for_each_safe(n, p, &tp->head) {
		list_del(n);
		destroy_task(list_entry(n, os_task_t, list));
	}

	free(tp->threads);
	free(tp);
}
