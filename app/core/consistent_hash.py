import hashlib
from bisect import bisect
from typing import List


class ConsistentHash:
    def __init__(self, nodes: List[str], virtual_nodes: int = 100):
        self.virtual_nodes = virtual_nodes
        self.hash_ring = {}
        self.sorted_keys = []

        for node in nodes:
            self.add_node(node)

    def _hash(self, key: str) -> int:
        """Helper to create a deterministic hash"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node: str) -> None:
        """Add a new node to the hash ring"""
        # TODO: Implement adding a new node
        # 1. Create virtual nodes for the new physical node
        # 2. Update hash_ring and sorted_keys
        # raise NotImplemented

        for i in range(self.virtual_nodes):
            virtual_key = f"{node}#{i}"
            hash_val = self._hash(virtual_key)
            
            self.hash_ring[hash_val] = node
            self.sorted_keys.append(hash_val)
        self.sorted_keys.sort()

    def get_node(self, key: str) -> str:
        """Get the node responsible for the given key"""
        if not self.hash_ring:
            # raise NotImplemented
            return None

        # TODO: Implement node lookup
        # 1. Calculate hash of the key
        # 2. Find the first node in the ring that comes after the key's hash
        # 3. If no such node exists, wrap around to the first node
        # raise NotImplemented

        hash_val = self._hash(key)
        
        idx = bisect(self.sorted_keys, hash_val)
        
        if idx == len(self.sorted_keys):
            idx = 0
            
        target_hash = self.sorted_keys[idx]
        return self.hash_ring[target_hash]
