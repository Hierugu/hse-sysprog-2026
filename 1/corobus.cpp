#include "corobus.h"

#include "libcoro.h"
#include "rlist.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

/**
 * One coroutine waiting to be woken up in a list of other
 * suspended coros.
 */
struct wakeup_entry {
	struct rlist base;
	struct coro *coro;
	bool already_removed;
};

/** A queue of suspended coros waiting to be woken up. */
struct wakeup_queue {
	struct rlist coros;
};

/** Suspend the current coroutine until it is woken up. */
static void
wakeup_queue_suspend_this(struct wakeup_queue *queue)
{
	struct wakeup_entry entry;
	entry.coro = coro_this();
	entry.already_removed = false;
	rlist_add_tail_entry(&queue->coros, &entry, base);
	coro_suspend();
	if (!entry.already_removed)
		rlist_del_entry(&entry, base);
}

/** Wakeup the first coroutine in the queue. */
static void
wakeup_queue_wakeup_first(struct wakeup_queue *queue)
{
	if (rlist_empty(&queue->coros))
		return;
	struct wakeup_entry *entry = rlist_first_entry(&queue->coros,
		struct wakeup_entry, base);
	entry->already_removed = true;
	rlist_del_entry(entry, base);
	coro_wakeup(entry->coro);
}

struct coro_bus_channel {
	/** Channel max capacity. */
	size_t size_limit;
	/** Coroutines waiting until the channel is not full. */
	struct wakeup_queue send_queue;
	/** Coroutines waiting until the channel is not empty. */
	struct wakeup_queue recv_queue;
	/** Message queue. */
	unsigned *data;
	/** Current number of messages in the queue. */
	size_t data_count;
	/** Index of the first message (head pointer). */
	size_t data_head;
};

struct coro_bus {
	struct coro_bus_channel **channels;
	int channel_count;
	int channel_capacity;
};

static enum coro_bus_error_code global_error = CORO_BUS_ERR_NONE;

enum coro_bus_error_code
coro_bus_errno(void)
{
	return global_error;
}

void
coro_bus_errno_set(enum coro_bus_error_code err)
{
	global_error = err;
}

struct coro_bus *
coro_bus_new(void)
{
	struct coro_bus *bus = (struct coro_bus *)malloc(sizeof(struct coro_bus));
	bus->channels = NULL;
	bus->channel_count = 0;
	bus->channel_capacity = 0;
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return bus;
}

void
coro_bus_delete(struct coro_bus *bus)
{
	if (bus == NULL)
		return;
	
	for (int i = 0; i < bus->channel_count; ++i) {
		if (bus->channels[i] != NULL) {
			struct coro_bus_channel *ch = bus->channels[i];
			free(ch->data);
			free(ch);
		}
	}
	free(bus->channels);
	free(bus);
}

int
coro_bus_channel_open(struct coro_bus *bus, size_t size_limit)
{
	for (int i = 0; i < bus->channel_count; ++i) {
		if (bus->channels[i] == NULL) {
			struct coro_bus_channel *ch = (struct coro_bus_channel *)
				malloc(sizeof(struct coro_bus_channel));
			ch->size_limit = size_limit;
			rlist_create(&ch->send_queue.coros);
			rlist_create(&ch->recv_queue.coros);
			ch->data = (unsigned *)malloc(sizeof(unsigned) * size_limit);
			ch->data_count = 0;
			ch->data_head = 0;
			bus->channels[i] = ch;
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			return i;
		}
	}

	int new_channel_id = bus->channel_count;
	
	if (bus->channel_count >= bus->channel_capacity) {
		int new_capacity;
		if (bus->channel_capacity == 0) {
			new_capacity = 4;
		} else if (bus->channel_capacity <= 1024) {
			new_capacity = bus->channel_capacity * 2;
		} else {
			new_capacity = bus->channel_capacity + bus->channel_capacity / 4;
		}
		bus->channels = (struct coro_bus_channel **)realloc(bus->channels,
			sizeof(struct coro_bus_channel *) * new_capacity);
		bus->channel_capacity = new_capacity;
	}
	
	struct coro_bus_channel *ch = (struct coro_bus_channel *)
		malloc(sizeof(struct coro_bus_channel));
	ch->size_limit = size_limit;
	rlist_create(&ch->send_queue.coros);
	rlist_create(&ch->recv_queue.coros);
	ch->data = (unsigned *)malloc(sizeof(unsigned) * size_limit);
	ch->data_count = 0;
	ch->data_head = 0;
	
	bus->channels[new_channel_id] = ch;
	bus->channel_count++;
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return new_channel_id;
}

void
coro_bus_channel_close(struct coro_bus *bus, int channel)
{
	if (channel < 0 || channel >= bus->channel_count)
		return;
	
	struct coro_bus_channel *ch = bus->channels[channel];
	if (ch == NULL)
		return;
	
	bus->channels[channel] = NULL;
	
	while (!rlist_empty(&ch->send_queue.coros)) {
		wakeup_queue_wakeup_first(&ch->send_queue);
	}
	while (!rlist_empty(&ch->recv_queue.coros)) {
		wakeup_queue_wakeup_first(&ch->recv_queue);
	}
	
	coro_yield();
	
	free(ch->data);
	free(ch);
}

int
coro_bus_send(struct coro_bus *bus, int channel, unsigned data)
{
	while (true) {
		if (channel < 0 || channel >= bus->channel_count ||
		    bus->channels[channel] == NULL) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
		
		int rc = coro_bus_try_send(bus, channel, data);
		if (rc == 0)
			return 0;
		
		enum coro_bus_error_code err = coro_bus_errno();
		if (err != CORO_BUS_ERR_WOULD_BLOCK)
			return -1;
		
		struct coro_bus_channel *ch = bus->channels[channel];
		wakeup_queue_suspend_this(&ch->send_queue);
	}
}

int
coro_bus_try_send(struct coro_bus *bus, int channel, unsigned data)
{
	if (channel < 0 || channel >= bus->channel_count) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	
	struct coro_bus_channel *ch = bus->channels[channel];
	if (ch == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	
	if (ch->data_count >= ch->size_limit) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	
	size_t tail = (ch->data_head + ch->data_count) % ch->size_limit;
	ch->data[tail] = data;
	ch->data_count++;
	
	wakeup_queue_wakeup_first(&ch->recv_queue);
	
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return 0;
}

int
coro_bus_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	while (true) {
		if (channel < 0 || channel >= bus->channel_count ||
		    bus->channels[channel] == NULL) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
		
		int rc = coro_bus_try_recv(bus, channel, data);
		if (rc == 0)
			return 0;
		
		enum coro_bus_error_code err = coro_bus_errno();
		if (err != CORO_BUS_ERR_WOULD_BLOCK)
			return -1;
		
		struct coro_bus_channel *ch = bus->channels[channel];
		wakeup_queue_suspend_this(&ch->recv_queue);
	}
}

int
coro_bus_try_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	if (channel < 0 || channel >= bus->channel_count) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	
	struct coro_bus_channel *ch = bus->channels[channel];
	if (ch == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	
	if (ch->data_count == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	
	*data = ch->data[ch->data_head];
	ch->data_head = (ch->data_head + 1) % ch->size_limit;
	ch->data_count--;
	
	wakeup_queue_wakeup_first(&ch->send_queue);
	
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return 0;
}


#if NEED_BROADCAST

int
coro_bus_try_broadcast(struct coro_bus *bus, unsigned data)
{
	int active_count = 0;
	for (int i = 0; i < bus->channel_count; ++i) {
		if (bus->channels[i] != NULL)
			active_count++;
	}
	
	if (active_count == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	
	for (int i = 0; i < bus->channel_count; ++i) {
		struct coro_bus_channel *ch = bus->channels[i];
		if (ch != NULL && ch->data_count >= ch->size_limit) {
			coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
			return -1;
		}
	}
	
	for (int i = 0; i < bus->channel_count; ++i) {
		struct coro_bus_channel *ch = bus->channels[i];
		if (ch == NULL)
			continue;
		
		size_t tail = (ch->data_head + ch->data_count) % ch->size_limit;
		ch->data[tail] = data;
		ch->data_count++;
		
		wakeup_queue_wakeup_first(&ch->recv_queue);
	}
	
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return 0;
}

int
coro_bus_broadcast(struct coro_bus *bus, unsigned data)
{
	while (true) {
		int rc = coro_bus_try_broadcast(bus, data);
		if (rc == 0)
			return 0;
		
		enum coro_bus_error_code err = coro_bus_errno();
		if (err != CORO_BUS_ERR_WOULD_BLOCK)
			return -1;
		
		for (int i = 0; i < bus->channel_count; ++i) {
			struct coro_bus_channel *ch = bus->channels[i];
			if (ch != NULL && ch->data_count >= ch->size_limit) {
				wakeup_queue_suspend_this(&ch->send_queue);
				break;
			}
		}
	}
}

#endif

#if NEED_BATCH

int
coro_bus_try_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
	if (channel < 0 || channel >= bus->channel_count) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	
	struct coro_bus_channel *ch = bus->channels[channel];
	if (ch == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	
	if (ch->data_count >= ch->size_limit) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	
	unsigned sent = 0;
	while (sent < count && ch->data_count < ch->size_limit) {
		size_t tail = (ch->data_head + ch->data_count) % ch->size_limit;
		ch->data[tail] = data[sent];
		ch->data_count++;
		sent++;
	}
	
	for (unsigned i = 0; i < sent; ++i) {
		wakeup_queue_wakeup_first(&ch->recv_queue);
	}
	
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return (int)sent;
}

int
coro_bus_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
	while (true) {
		if (channel < 0 || channel >= bus->channel_count ||
		    bus->channels[channel] == NULL) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
		
		int rc = coro_bus_try_send_v(bus, channel, data, count);
		if (rc > 0)
			return rc;
		
		enum coro_bus_error_code err = coro_bus_errno();
		if (err != CORO_BUS_ERR_WOULD_BLOCK)
			return -1;
		
		struct coro_bus_channel *ch = bus->channels[channel];
		wakeup_queue_suspend_this(&ch->send_queue);
	}
}

int
coro_bus_try_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
	if (channel < 0 || channel >= bus->channel_count) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	
	struct coro_bus_channel *ch = bus->channels[channel];
	if (ch == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	
	if (ch->data_count == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	
	unsigned received = 0;
	while (received < capacity && ch->data_count > 0) {
		data[received] = ch->data[ch->data_head];
		ch->data_head = (ch->data_head + 1) % ch->size_limit;
		ch->data_count--;
		received++;
	}
	
	for (unsigned i = 0; i < received; ++i) {
		wakeup_queue_wakeup_first(&ch->send_queue);
	}
	
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return (int)received;
}

int
coro_bus_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
	while (true) {
		if (channel < 0 || channel >= bus->channel_count ||
		    bus->channels[channel] == NULL) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
		
		int rc = coro_bus_try_recv_v(bus, channel, data, capacity);
		if (rc > 0)
			return rc;
		
		enum coro_bus_error_code err = coro_bus_errno();
		if (err != CORO_BUS_ERR_WOULD_BLOCK)
			return -1;
		
		struct coro_bus_channel *ch = bus->channels[channel];
		wakeup_queue_suspend_this(&ch->recv_queue);
	}
}

#endif
