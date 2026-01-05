import asyncio
import time
from collections import defaultdict
from typing import Dict

from app.core.config import settings
from app.core.redis_manager import RedisManager


class PollingService:
    # In-memory storage for Task 1 & Task 4 (Batch buffer)
    _memory_storage = defaultdict(lambda: defaultdict(int))

    def __init__(self):
        self.redis_manager = RedisManager()
        self._cache = {}
        self.CACHE_TTL = 5  # 5 seconds

    async def vote(self, poll_id: str, option_id: str) -> None:
        """
        Registers a vote.
        Task 1: Store in memory.
        Task 2: Write to Redis immediately.
        Task 4: Buffer in memory (Batching).
        """
        # TODO: Implement vote logic based on the current task
        # raise NotImplemented

        # self._memory_storage[poll_id][option_id] += 1

        # client = await self.redis_manager.get_client(poll_id)
        # await client.hincrby(poll_id, option_id, 1)

        self._memory_storage[poll_id][option_id] += 1

    async def get_results(self, poll_id: str) -> Dict[str, int]:
        """
        Get results.
        Task 1: Read from memory.
        Task 2: Read from Redis.
        Task 3: Check App Cache -> Redis.
        Task 4: Redis + Memory Buffer.
        """
        # TODO: Implement result fetching logic
        # Should return a dictionary like {"OptionA": 5, "OptionB": 3}
        # raise NotImplemented

        # return self._memory_storage[poll_id]

        # client = await self.redis_manager.get_client(poll_id)
        # raw_results = await client.hgetall(poll_id)
        # return {k: int(v) for k, v in raw_results.items()}

        # current_time = time.time()
        # if poll_id in self._cache:
        #     timestamp, cached_data = self._cache[poll_id]
        #     if current_time - timestamp < self.CACHE_TTL:
        #         return cached_data, "app_cache"
            
        # client = await self.redis_manager.get_client(poll_id)
        # raw_results = await client.hgetall(poll_id)
        # results = {k: int(v) for k, v in raw_results.items()}
        # self._cache[poll_id] = (current_time, results)
        # return results, "redis"

       
        # client = await self.redis_manager.get_client(poll_id)
        # raw_results = await client.hgetall(poll_id)
        # results = {k: int(v) for k, v in raw_results.items()}
        
        # if poll_id in self._memory_storage:
        #      for option, count in self._memory_storage[poll_id].items():
        #          results[option] = results.get(option, 0) + count
        
        # return results, "redis"


        current_time = time.time()
        if poll_id in self._cache:
            timestamp, cached_data = self._cache[poll_id]
            if current_time - timestamp < self.CACHE_TTL:
                return cached_data, "app_cache"
        
        node_url = self.redis_manager.consistent_hash.get_node(poll_id)
        debug_source = f"redis_{node_url}"

        client = await self.redis_manager.get_client(poll_id)
        raw_results = await client.hgetall(poll_id)
        results = {k: int(v) for k, v in raw_results.items()}

        if poll_id in self._memory_storage:
             for option, count in self._memory_storage[poll_id].items():
                 results[option] = results.get(option, 0) + count

        self._cache[poll_id] = (current_time, results)
        return results, debug_source



    async def flush_batch(self):
        """
        Task 4: Background process to flush memory buffer to Redis.
        """
        # TODO: Implement the batch flushing loop
        # 1. Loop forever (while True)
        # 2. Wait for BATCH_INTERVAL_SECONDS
        # 3. Flush _memory_storage to Redis
        # raise NotImplemented
        while True:
            await asyncio.sleep(settings.BATCH_INTERVAL_SECONDS)
            
            if not self._memory_storage:
                continue
                
            print(f"Flushing {len(self._memory_storage)} polls to Redis...")

            polls_to_flush = dict(self._memory_storage)
            self._memory_storage.clear()
            
            for poll_id, votes in polls_to_flush.items():
                try:
                    client = await self.redis_manager.get_client(poll_id)
                    async with client.pipeline() as pipe:
                        for option_id, count in votes.items():
                            pipe.hincrby(poll_id, option_id, count)
                        await pipe.execute()
                except Exception as e:
                    print(f"Error flushing poll {poll_id}: {e}")