#include "duckdb/storage/buffer/standard_buffer_pool.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

StandardBufferPool::StandardBufferPool(idx_t maximum_memory)
    : current_memory(0), maximum_memory(maximum_memory), queue(make_unique<EvictionQueue>()), queue_insertions(0) {
}
StandardBufferPool::~StandardBufferPool() {
}

BufferPool::EvictionResult StandardBufferPool::EvictBlocks(idx_t extra_memory, idx_t memory_limit,
                                                           unique_ptr<FileBuffer> *buffer) {
	BufferEvictionNode node;
	TempBufferPoolReservation reservation(*this, extra_memory);
	while (current_memory > memory_limit) {
		// get a block to unpin from the queue
		if (!queue->q.try_dequeue(node)) {
			// Failed to reserve. Adjust size of temp reservation to 0.
			reservation.Resize(0);
			return {false, std::move(reservation)};
		}
		// get a reference to the underlying block pointer
		auto handle = node.TryGetBlockHandle();
		if (!handle) {
			continue;
		}
		// we might be able to free this block: grab the mutex and check if we can free it
		lock_guard<mutex> lock(handle->lock);
		if (!node.CanUnload(*handle)) {
			// something changed in the mean-time, bail out
			continue;
		}
		// hooray, we can unload the block
		if (buffer && handle->buffer->AllocSize() == extra_memory) {
			// we can actually re-use the memory directly!
			*buffer = handle->UnloadAndTakeBlock();
			return {true, std::move(reservation)};
		} else {
			// release the memory and mark the block as unloaded
			handle->Unload();
		}
	}
	return {true, std::move(reservation)};
}

void StandardBufferPool::PurgeQueue() {
	BufferEvictionNode node;
	while (true) {
		if (!queue->q.try_dequeue(node)) {
			break;
		}
		auto handle = node.TryGetBlockHandle();
		if (!handle) {
			continue;
		} else {
			queue->q.enqueue(std::move(node));
			break;
		}
	}
}

void StandardBufferPool::SetLimit(idx_t limit, const char *exception_postscript) {
	lock_guard<mutex> l_lock(limit_lock);
	// try to evict until the limit is reached
	if (!EvictBlocks(0, limit).success) {
		throw OutOfMemoryException(
		    "Failed to change memory limit to %lld: could not free up enough memory for the new limit%s", limit,
		    exception_postscript);
	}
	idx_t old_limit = maximum_memory;
	// set the global maximum memory to the new limit if successful
	maximum_memory = limit;
	// evict again
	if (!EvictBlocks(0, limit).success) {
		// failed: go back to old limit
		maximum_memory = old_limit;
		throw OutOfMemoryException(
		    "Failed to change memory limit to %lld: could not free up enough memory for the new limit%s", limit,
		    exception_postscript);
	}
}

void StandardBufferPool::AddToEvictionQueue(shared_ptr<BlockHandle> &handle) {
	constexpr int INSERT_INTERVAL = 1024;

	D_ASSERT(handle->readers == 0);
	handle->eviction_timestamp++;
	// After each 1024 insertions, run through the queue and purge.
	if ((++queue_insertions % INSERT_INTERVAL) == 0) {
		PurgeQueue();
	}
	queue->q.enqueue(BufferEvictionNode(weak_ptr<BlockHandle>(handle), handle->eviction_timestamp));
}

} // namespace duckdb
