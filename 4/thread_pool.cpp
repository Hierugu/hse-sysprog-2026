#include "thread_pool.h"

#include <pthread.h>
#include <queue>
#include <time.h>

enum task_state {
	TASK_NEW,
	TASK_PUSHED,
	TASK_RUNNING,
	TASK_FINISHED,
	TASK_JOINED
};

struct thread_task {
	thread_task_f function;
	
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	
	enum task_state state;
	bool is_detached;
	struct thread_pool *pool;
};

struct thread_pool {
	std::vector<pthread_t> threads;
	std::queue<struct thread_task*> task_queue;
	
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	
	int max_threads;
	int active_threads;
	int task_count;
	bool is_shutting_down;
};

static void*
worker_thread(void *arg)
{
	struct thread_pool *pool = (struct thread_pool*)arg;
	
	while (true) {
		pthread_mutex_lock(&pool->mutex);
		
		while (pool->task_queue.empty() && !pool->is_shutting_down) {
			pthread_cond_wait(&pool->cond, &pool->mutex);
		}
		
		if (pool->is_shutting_down && pool->task_queue.empty()) {
			pthread_mutex_unlock(&pool->mutex);
			break;
		}
		
		struct thread_task *task = pool->task_queue.front();
		pool->task_queue.pop();
		pthread_mutex_unlock(&pool->mutex);
		
		pthread_mutex_lock(&task->mutex);
		task->state = TASK_RUNNING;
		pthread_mutex_unlock(&task->mutex);
		
		task->function();
		
		pthread_mutex_lock(&pool->mutex);
		pool->task_count--;
		pthread_mutex_unlock(&pool->mutex);
		
		pthread_mutex_lock(&task->mutex);
		task->state = TASK_FINISHED;
		bool detached = task->is_detached;
		pthread_cond_broadcast(&task->cond);
		pthread_mutex_unlock(&task->mutex);
		
		if (detached) {
			pthread_mutex_destroy(&task->mutex);
			pthread_cond_destroy(&task->cond);
			delete task;
		}
	}
	
	return NULL;
}

int
thread_pool_new(int thread_count, struct thread_pool **pool)
{
	if (thread_count <= 0 || thread_count > TPOOL_MAX_THREADS) {
		return TPOOL_ERR_INVALID_ARGUMENT;
	}
	
	struct thread_pool *p = new thread_pool;
	pthread_mutex_init(&p->mutex, NULL);
	pthread_cond_init(&p->cond, NULL);
	
	p->max_threads = thread_count;
	p->active_threads = 0;
	p->task_count = 0;
	p->is_shutting_down = false;
	
	*pool = p;
	return 0;
}

int
thread_pool_delete(struct thread_pool *pool)
{
	pthread_mutex_lock(&pool->mutex);
	
	if (pool->task_count > 0) {
		pthread_mutex_unlock(&pool->mutex);
		return TPOOL_ERR_HAS_TASKS;
	}
	
	pool->is_shutting_down = true;
	pthread_cond_broadcast(&pool->cond);
	pthread_mutex_unlock(&pool->mutex);
	
	for (size_t i = 0; i < pool->threads.size(); ++i) {
		pthread_join(pool->threads[i], NULL);
	}
	
	pthread_mutex_destroy(&pool->mutex);
	pthread_cond_destroy(&pool->cond);
	delete pool;
	
	return 0;
}

int
thread_pool_push_task(struct thread_pool *pool, struct thread_task *task)
{
	pthread_mutex_lock(&task->mutex);
	task->state = TASK_PUSHED;
	task->pool = pool;
	pthread_mutex_unlock(&task->mutex);
	
	pthread_mutex_lock(&pool->mutex);
	
	if (pool->task_count >= TPOOL_MAX_TASKS) {
		pthread_mutex_unlock(&pool->mutex);
		pthread_mutex_lock(&task->mutex);
		task->state = TASK_NEW;
		task->pool = NULL;
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TOO_MANY_TASKS;
	}
	
	pool->task_queue.push(task);
	pool->task_count++;
	
	if (pool->active_threads < pool->max_threads &&
	    (size_t)pool->active_threads < pool->task_queue.size()) {
		pthread_t thread;
		pthread_create(&thread, NULL, worker_thread, pool);
		pool->threads.push_back(thread);
		pool->active_threads++;
	}
	
	pthread_cond_signal(&pool->cond);
	pthread_mutex_unlock(&pool->mutex);
	
	return 0;
}

int
thread_task_new(struct thread_task **task, const thread_task_f &function)
{
	struct thread_task *t = new thread_task;
	t->function = function;
	t->state = TASK_NEW;
	t->is_detached = false;
	t->pool = NULL;
	
	pthread_mutex_init(&t->mutex, NULL);
	pthread_cond_init(&t->cond, NULL);
	
	*task = t;
	return 0;
}

bool
thread_task_is_finished(const struct thread_task *task)
{
	pthread_mutex_lock((pthread_mutex_t*)&task->mutex);
	bool finished = (task->state == TASK_FINISHED || task->state == TASK_JOINED);
	pthread_mutex_unlock((pthread_mutex_t*)&task->mutex);
	return finished;
}

bool
thread_task_is_running(const struct thread_task *task)
{
	pthread_mutex_lock((pthread_mutex_t*)&task->mutex);
	bool running = (task->state == TASK_RUNNING);
	pthread_mutex_unlock((pthread_mutex_t*)&task->mutex);
	return running;
}

int
thread_task_join(struct thread_task *task)
{
	pthread_mutex_lock(&task->mutex);
	
	if (task->state == TASK_NEW) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}
	
	while (task->state != TASK_FINISHED && task->state != TASK_JOINED) {
		pthread_cond_wait(&task->cond, &task->mutex);
	}
	
	task->state = TASK_JOINED;
	pthread_mutex_unlock(&task->mutex);
	
	return 0;
}

#if NEED_TIMED_JOIN

int
thread_task_timed_join(struct thread_task *task, double timeout)
{
	pthread_mutex_lock(&task->mutex);
	
	if (task->state == TASK_NEW) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}
	
	if (task->state == TASK_FINISHED || task->state == TASK_JOINED) {
		task->state = TASK_JOINED;
		pthread_mutex_unlock(&task->mutex);
		return 0;
	}
	
	if (timeout <= 0) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TIMEOUT;
	}
	
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	
	long long nsec = ts.tv_nsec + (long long)(timeout * 1000000000.0);
	ts.tv_sec += nsec / 1000000000;
	ts.tv_nsec = nsec % 1000000000;
	
	int rc = 0;
	while (task->state != TASK_FINISHED && task->state != TASK_JOINED) {
		rc = pthread_cond_timedwait(&task->cond, &task->mutex, &ts);
		if (rc == ETIMEDOUT) {
			pthread_mutex_unlock(&task->mutex);
			return TPOOL_ERR_TIMEOUT;
		}
	}
	
	task->state = TASK_JOINED;
	pthread_mutex_unlock(&task->mutex);
	
	return 0;
}

#endif

int
thread_task_delete(struct thread_task *task)
{
	pthread_mutex_lock(&task->mutex);
	
	if (task->state == TASK_PUSHED || task->state == TASK_RUNNING) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_IN_POOL;
	}
	
	pthread_mutex_unlock(&task->mutex);
	
	pthread_mutex_destroy(&task->mutex);
	pthread_cond_destroy(&task->cond);
	delete task;
	
	return 0;
}

#if NEED_DETACH

int
thread_task_detach(struct thread_task *task)
{
	pthread_mutex_lock(&task->mutex);
	
	if (task->state == TASK_NEW) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}
	
	if (task->state == TASK_FINISHED || task->state == TASK_JOINED) {
		pthread_mutex_unlock(&task->mutex);
		pthread_mutex_destroy(&task->mutex);
		pthread_cond_destroy(&task->cond);
		delete task;
		return 0;
	}
	
	task->is_detached = true;
	pthread_mutex_unlock(&task->mutex);
	
	return 0;
}

#endif
