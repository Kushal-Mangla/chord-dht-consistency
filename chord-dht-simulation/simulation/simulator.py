from typing import Dict, List, Optional, Any
from chord.node import Node
import time

class Simulator:
    def __init__(self, m=8):
        """
        Initialize the Chord DHT simulator.
        m: Number of bits in the hash space (default 8, meaning 0-255 range)
        """
        self.m = m
        self.ring_size = 2 ** m
        self.nodes = {}  # node_id -> Node object
        self.event_log = []
        self.replication_factor = 3
        self.hinted_handoffs = {}  # node_id -> {key: (value, target_node)}
        
    def _hash(self, key: str) -> int:
        """Hash a key to an integer in the ring space using simple modulo."""
        # Try to convert key to integer, if it fails use ord() of first char
        try:
            key_int = int(key)
        except ValueError:
            # For non-numeric keys, use the sum of character codes
            key_int = sum(ord(c) for c in key)
        return key_int % self.ring_size
    
    def _get_sorted_node_ids(self) -> List[int]:
        """Get all node IDs sorted in ascending order."""
        return sorted([int(nid) for nid in self.nodes.keys()])
    
    def _find_successor(self, key_hash: int) -> Optional[int]:
        """Find the successor node for a given key hash."""
        sorted_ids = self._get_sorted_node_ids()
        if not sorted_ids:
            return None
        
        for node_id in sorted_ids:
            if node_id >= key_hash:
                return node_id
        # Wrap around to the first node
        return sorted_ids[0]
    
    def _get_replicas(self, primary_node_id: int, count: int = None) -> List[int]:
        """Get the list of replica nodes (successors)."""
        if count is None:
            count = self.replication_factor
        
        sorted_ids = self._get_sorted_node_ids()
        if not sorted_ids:
            return []
        
        replicas = []
        try:
            idx = sorted_ids.index(primary_node_id)
            for i in range(1, count):
                replicas.append(sorted_ids[(idx + i) % len(sorted_ids)])
        except ValueError:
            pass
        
        return replicas

    def join_node(self, node_id: str, address: str = None):
        """Add a new node to the ring."""
        node_id_int = int(node_id)
        
        if node_id in self.nodes:
            self.event_log.append({
                'timestamp': time.time(),
                'type': 'error',
                'message': f"Node {node_id} already exists."
            })
            return False
        
        if address is None:
            address = f"127.0.0.1:{5000 + node_id_int}"
        
        self.nodes[node_id] = Node(node_id_int, address)
        self.event_log.append({
            'timestamp': time.time(),
            'type': 'node_join',
            'node_id': node_id,
            'message': f"Node {node_id} joined at {address}."
        })
        
        # Redistribute keys if necessary
        self._redistribute_keys(node_id_int)
        
        # Update replicas for existing keys
        self._update_all_replicas()
        
        return True

    def _redistribute_keys(self, new_node_id: int):
        """Redistribute keys to the new node if they belong to it."""
        sorted_ids = self._get_sorted_node_ids()
        if len(sorted_ids) <= 1:
            return
        
        new_node = self.nodes[str(new_node_id)]
        keys_moved = 0
        
        # Check ALL nodes to see if any of their keys should now belong to the new node
        for node_id_str in list(self.nodes.keys()):
            node_id_int = int(node_id_str)
            if node_id_int == new_node_id:
                continue  # Skip the new node itself
            
            node = self.nodes[node_id_str]
            keys_to_move = []
            
            # Check each key in this node
            for key, value in list(node.key_value_store.items()):
                key_hash = self._hash(key)
                successor = self._find_successor(key_hash)
                
                # If this key's successor is now the new node, move it
                if successor == new_node_id:
                    keys_to_move.append((key, value))
            
            # Move the keys
            for key, value in keys_to_move:
                del node.key_value_store[key]
                new_node.key_value_store[key] = value
                keys_moved += 1
        
        if keys_moved > 0:
            self.event_log.append({
                'timestamp': time.time(),
                'type': 'key_redistribution',
                'node_id': str(new_node_id),
                'message': f"Redistributed {keys_moved} key(s) to node {new_node_id}."
            })

    def leave_node(self, node_id: str):
        """Remove a node from the ring permanently."""
        if node_id not in self.nodes:
            self.event_log.append({
                'timestamp': time.time(),
                'type': 'error',
                'message': f"Node {node_id} does not exist."
            })
            return False
        
        # Transfer keys to successor before removal
        node = self.nodes[node_id]
        successor_id = self._get_replicas(int(node_id), 1)
        if successor_id and node.key_value_store:
            successor_node = self.nodes[str(successor_id[0])]
            for key, value in node.key_value_store.items():
                successor_node.key_value_store[key] = value
        
        del self.nodes[node_id]
        self.event_log.append({
            'timestamp': time.time(),
            'type': 'node_leave',
            'node_id': node_id,
            'message': f"Node {node_id} left the ring."
        })
        return True

    def simulate_offline(self, node_id: str):
        """Simulate a node going offline."""
        if node_id not in self.nodes:
            self.event_log.append({
                'timestamp': time.time(),
                'type': 'error',
                'message': f"Node {node_id} does not exist."
            })
            return False
        
        self.nodes[node_id].go_offline()
        self.event_log.append({
            'timestamp': time.time(),
            'type': 'node_offline',
            'node_id': node_id,
            'message': f"Node {node_id} went offline."
        })
        return True

    def simulate_online(self, node_id: str):
        """Bring a node back online."""
        if node_id not in self.nodes:
            self.event_log.append({
                'timestamp': time.time(),
                'type': 'error',
                'message': f"Node {node_id} does not exist."
            })
            return False
        
        self.nodes[node_id].come_online()
        
        # Apply hinted handoffs
        if node_id in self.hinted_handoffs:
            for key, (value, _) in self.hinted_handoffs[node_id].items():
                self.nodes[node_id].key_value_store[key] = value
            del self.hinted_handoffs[node_id]
            self.event_log.append({
                'timestamp': time.time(),
                'type': 'hinted_handoff',
                'node_id': node_id,
                'message': f"Node {node_id} recovered with hinted handoffs."
            })
        else:
            self.event_log.append({
                'timestamp': time.time(),
                'type': 'node_online',
                'node_id': node_id,
                'message': f"Node {node_id} came back online."
            })
        return True

    def put(self, key: str, value: Any, write_quorum: int = 2):
        """Store a key-value pair with replication."""
        key_hash = self._hash(key)
        primary_node_id = self._find_successor(key_hash)
        
        if primary_node_id is None:
            self.event_log.append({
                'timestamp': time.time(),
                'type': 'error',
                'message': f"No nodes available to store key '{key}'."
            })
            return False
        
        # Get replica nodes
        replica_ids = [primary_node_id] + self._get_replicas(primary_node_id, self.replication_factor)
        
        successful_writes = 0
        nodes_written = []
        
        for node_id in replica_ids[:self.replication_factor]:
            node = self.nodes[str(node_id)]
            if node.is_online:
                result = node.put(key, value)
                if result:
                    successful_writes += 1
                    nodes_written.append(str(node_id))
            else:
                # Hinted handoff - store hint for offline node
                if str(node_id) not in self.hinted_handoffs:
                    self.hinted_handoffs[str(node_id)] = {}
                self.hinted_handoffs[str(node_id)][key] = (value, str(primary_node_id))
        
        if successful_writes >= write_quorum:
            self.event_log.append({
                'timestamp': time.time(),
                'type': 'put_success',
                'key': key,
                'value': value,
                'hash': key_hash,
                'nodes': nodes_written,
                'message': f"Stored key '{key}' = '{value}' on {successful_writes} nodes (hash: {key_hash})."
            })
            return True
        else:
            self.event_log.append({
                'timestamp': time.time(),
                'type': 'put_failure',
                'key': key,
                'message': f"Failed to meet write quorum for key '{key}'."
            })
            return False

    def get(self, key: str, read_quorum: int = 2):
        """Retrieve a value by key with quorum reads."""
        key_hash = self._hash(key)
        primary_node_id = self._find_successor(key_hash)
        
        if primary_node_id is None:
            self.event_log.append({
                'timestamp': time.time(),
                'type': 'error',
                'message': f"No nodes available to retrieve key '{key}'."
            })
            return None
        
        # Get replica nodes
        replica_ids = [primary_node_id] + self._get_replicas(primary_node_id, self.replication_factor)
        
        values = {}
        for node_id in replica_ids[:self.replication_factor]:
            node = self.nodes[str(node_id)]
            if node.is_online:
                value = node.get(key)
                if value is not None:
                    if value not in values:
                        values[value] = 0
                    values[value] += 1
        
        if not values:
            self.event_log.append({
                'timestamp': time.time(),
                'type': 'get_failure',
                'key': key,
                'message': f"Key '{key}' not found."
            })
            return None
        
        # Return the most common value
        result_value = max(values.items(), key=lambda x: x[1])[0]
        
        if values[result_value] >= read_quorum:
            self.event_log.append({
                'timestamp': time.time(),
                'type': 'get_success',
                'key': key,
                'value': result_value,
                'hash': key_hash,
                'message': f"Retrieved key '{key}' = '{result_value}' (hash: {key_hash})."
            })
            return result_value
        else:
            self.event_log.append({
                'timestamp': time.time(),
                'type': 'get_partial',
                'key': key,
                'value': result_value,
                'message': f"Retrieved key '{key}' but did not meet read quorum."
            })
            return result_value

    def get_nodes(self) -> Dict:
        """Get all nodes and their states."""
        nodes_data = {}
        for node_id, node in self.nodes.items():
            successor = self._find_successor(int(node_id))
            replicas = self._get_replicas(int(node_id))
            
            nodes_data[node_id] = {
                'node_id': node_id,
                'address': node.address,
                'state': 'online' if node.is_online else 'offline',
                'storage': dict(node.key_value_store),
                'successor': str(successor) if successor else None,
                'replicas': [str(r) for r in replicas],
                'key_count': len(node.key_value_store)
            }
        return nodes_data

    def get_event_log(self, limit: int = 50) -> List[Dict]:
        """Get recent event log entries."""
        return self.event_log[-limit:]

    def reset_event_log(self):
        """Clear the event log."""
        self.event_log = []
        
    def get_ring_data(self) -> Dict:
        """Get data for ring visualization."""
        sorted_ids = self._get_sorted_node_ids()
        return {
            'ring_size': self.ring_size,
            'node_ids': sorted_ids,
            'nodes': self.get_nodes()
        }
    
    def _update_all_replicas(self):
        """Update replicas across all nodes after ring topology changes."""
        # Collect all unique keys across the ring
        all_keys = {}
        for node_id_str, node in self.nodes.items():
            for key, value in node.key_value_store.items():
                if key not in all_keys:
                    all_keys[key] = value
        
        # For each key, ensure it's replicated correctly
        for key, value in all_keys.items():
            key_hash = self._hash(key)
            primary_node_id = self._find_successor(key_hash)
            
            if primary_node_id is None:
                continue
            
            # Get the correct replica nodes for this key
            replica_ids = [primary_node_id] + self._get_replicas(primary_node_id, self.replication_factor)
            replica_ids = replica_ids[:self.replication_factor]
            
            # Ensure the key exists on all replica nodes (if they're online)
            for replica_id in replica_ids:
                replica_node = self.nodes[str(replica_id)]
                if replica_node.is_online and key not in replica_node.key_value_store:
                    replica_node.key_value_store[key] = value
            
            # Remove key from nodes that shouldn't have it
            for node_id_str, node in self.nodes.items():
                node_id_int = int(node_id_str)
                if node_id_int not in replica_ids and key in node.key_value_store:
                    # Only remove if this node is not a valid replica
                    del node.key_value_store[key]