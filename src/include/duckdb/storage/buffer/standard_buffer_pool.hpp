#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/parallel/concurrentqueue.hpp"
#include "duckdb/common/file_buffer.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"

namespace duckdb {

struct BufferEvictionNode {
	BufferEvictionNode() {
	}
	BufferEvictionNode(weak_ptr<BlockHandle> handle_p, idx_t timestamp_p)
	    : handle(std::move(handle_p)), timestamp(timestamp_p) {
		D_ASSERT(!handle.expired());
	}

	weak_ptr<BlockHandle> handle;
	idx_t timestamp;

	bool CanUnload(BlockHandle &handle_p) {
		if (timestamp != handle_p.eviction_timestamp) {
			// handle was used in between
			return false;
		}
		return handle_p.CanUnload();
	}

	shared_ptr<BlockHandle> TryGetBlockHandle() {
		auto handle_p = handle.lock();
		if (!handle_p) {
			// BlockHandle has been destroyed
			return nullptr;
		}
		if (!CanUnload(*handle_p)) {
			// handle was used in between
			return nullptr;
		}
		// this is the latest node in the queue with this handle
		return handle_p;
	}
};

typedef duckdb_moodycamel::ConcurrentQueue<BufferEvictionNode> eviction_queue_t;

struct EvictionQueue {
	eviction_queue_t q;
};

//! The StandardBufferPool is in charge of handling memory management for one or more databases. It defines memory
//! limits and implements priority eviction among all users of the pool.
class StandardBufferPool : public BufferPool {
	friend class BlockHandle;
	friend class BlockManager;
	friend class BufferManager;
	friend class StandardBufferManager;

public:
	explicit StandardBufferPool(idx_t maximum_memory);
	virtual ~StandardBufferPool();

	//! Set a new memory limit to the buffer pool, throws an exception if the new limit is too low and not enough
	//! blocks can be evicted
	void SetLimit(idx_t limit, const char *exception_postscript) final override;

	idx_t GetUsedMemory() final override {
		return current_memory;
	}
	idx_t GetMaxMemory() final override {
		return maximum_memory;
	}

	void IncreaseUsedMemory(idx_t size) final override {
		current_memory += size;
	}
	void DecreaseUsedMemory(idx_t size) final override {
		// TODO: assert that size < current_memory ?
		current_memory -= size;
	}

protected:
	//! Evict blocks until the currently used memory + extra_memory fit, returns false if this was not possible
	//! (i.e. not enough blocks could be evicted)
	//! If the "buffer" argument is specified AND the system can find a buffer to re-use for the given allocation size
	//! "buffer" will be made to point to the re-usable memory. Note that this is not guaranteed.
	//! Returns a pair. result.first indicates if eviction was successful. result.second contains the
	//! reservation handle, which can be moved to the BlockHandle that will own the reservation.
	virtual BufferPool::EvictionResult EvictBlocks(idx_t extra_memory, idx_t memory_limit,
	                                               unique_ptr<FileBuffer> *buffer = nullptr) override;

	//! Garbage collect eviction queue
	void PurgeQueue() final override;
	void AddToEvictionQueue(shared_ptr<BlockHandle> &handle) final override;

private:
	//! The lock for changing the memory limit
	mutex limit_lock;
	//! The current amount of memory that is occupied by the buffer manager (in bytes)
	atomic<idx_t> current_memory;
	//! The maximum amount of memory that the buffer manager can keep (in bytes)
	atomic<idx_t> maximum_memory;
	//! Eviction queue
	unique_ptr<EvictionQueue> queue;
	//! Total number of insertions into the eviction queue. This guides the schedule for calling PurgeQueue.
	atomic<uint32_t> queue_insertions;
};

} // namespace duckdb
