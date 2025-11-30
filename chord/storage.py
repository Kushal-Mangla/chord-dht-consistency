"""
Local key-value storage for Chord node with version tracking.
ENHANCED: Now supports persistent storage with primary/backup separation.
"""

import hashlib
import os
import json
import pickle
from typing import Dict, Optional, Tuple, Any, List
from consistency.vector_clock import VectorClock


class ChordStorage:
    """
    Local storage for key-value pairs with version metadata.
    
    ENHANCED: Supports persistent storage with:
    - Primary storage: keys we are responsible for
    - Backup storage: replicas of keys from other nodes
    """
    
    def __init__(self, node_id: int, base_dir: str = "storage", enable_persistence: bool = False):
        """
        Initialize storage for a Chord node.
        
        Args:
            node_id: Identifier of the node owning this storage
            base_dir: Base directory for persistent storage
            enable_persistence: Whether to enable persistent storage to disk
        """
        self.node_id = node_id
        self.enable_persistence = enable_persistence
        
        # In-memory storage
        self.primary_store: Dict[str, Tuple[Any, VectorClock]] = {}
        self.backup_store: Dict[int, Dict[str, Tuple[Any, VectorClock]]] = {}  # node_id -> key -> (value, version)
        
        # Legacy store for backwards compatibility
        self.store: Dict[str, Tuple[Any, VectorClock]] = self.primary_store
        
        # Persistent storage paths
        if self.enable_persistence:
            self.storage_dir = os.path.join(base_dir, f"node_{node_id}")
            self.primary_dir = os.path.join(self.storage_dir, "primary")
            self.backup_dir = os.path.join(self.storage_dir, "backup")
            
            # Create directories
            os.makedirs(self.primary_dir, exist_ok=True)
            os.makedirs(self.backup_dir, exist_ok=True)
    
    def put(self, key: str, value: Any, version: Optional[VectorClock] = None) -> VectorClock:
        """
        Store a key-value pair with version metadata.
        
        Args:
            key: The key to store
            value: The value to store
            version: Vector clock version (if None, create new one)
            
        Returns:
            The version assigned to this put operation
        """
        if version is None:
            # Create new version or increment existing
            if key in self.primary_store:
                _, old_version = self.primary_store[key]
                # Copy the old version and increment
                version = VectorClock(old_version.clock.copy())
                version.increment(self.node_id)
            else:
                version = VectorClock()
                version.increment(self.node_id)
        
        self.primary_store[key] = (value, version)
        
        # Persist to disk if enabled
        if self.enable_persistence:
            self._save_primary(key, value, version)
        
        return version
    
    def put_backup(self, key: str, value: Any, version: VectorClock, for_node_id: int) -> VectorClock:
        """
        Store a backup replica for another node.
        
        Args:
            key: The key to store
            value: The value to store
            version: Vector clock version
            for_node_id: ID of the primary node for this key
            
        Returns:
            The version assigned to this backup
        """
        if for_node_id not in self.backup_store:
            self.backup_store[for_node_id] = {}
        
        self.backup_store[for_node_id][key] = (value, version)
        
        # Persist to disk if enabled
        if self.enable_persistence:
            self._save_backup(key, value, version, for_node_id)
        
        return version
    
    def get_backup(self, key: str, for_node_id: int) -> Optional[Tuple[Any, VectorClock]]:
        """
        Retrieve a backup replica.
        
        Args:
            key: The key to retrieve
            for_node_id: ID of the primary node
            
        Returns:
            Tuple of (value, version) or None
        """
        if for_node_id in self.backup_store:
            return self.backup_store[for_node_id].get(key)
        return None
    
    # ==================== Persistent Storage Methods ====================
    
    def _save_primary(self, key: str, value: Any, version: VectorClock):
        """Save a primary key to disk as human-readable text."""
        if not self.enable_persistence:
            return
        
        # Use .txt extension for human-readable files
        key_file = os.path.join(self.primary_dir, f"{key}.txt")
        with open(key_file, 'w') as f:
            data = {
                'key': key,
                'value': value,
                'version': version.to_dict(),
                'type': 'primary'
            }
            json.dump(data, f, indent=2)
    
    def _save_backup(self, key: str, value: Any, version: VectorClock, for_node_id: int):
        """Save a backup key to disk as human-readable text."""
        if not self.enable_persistence:
            return
        
        node_backup_dir = os.path.join(self.backup_dir, f"node_{for_node_id}")
        os.makedirs(node_backup_dir, exist_ok=True)
        
        key_file = os.path.join(node_backup_dir, f"{key}.txt")
        with open(key_file, 'w') as f:
            data = {
                'key': key,
                'value': value,
                'version': version.to_dict(),
                'type': 'backup',
                'primary_node_id': for_node_id
            }
            json.dump(data, f, indent=2)
    
    def load_all_primary(self) -> int:
        """
        Load all primary keys from disk on startup.
        
        Returns:
            Number of keys loaded
        """
        if not self.enable_persistence or not os.path.exists(self.primary_dir):
            return 0
        
        count = 0
        for filename in os.listdir(self.primary_dir):
            if filename.endswith('.txt'):
                key = filename[:-4]  # Remove .txt extension
                key_file = os.path.join(self.primary_dir, filename)
                try:
                    with open(key_file, 'r') as f:
                        data = json.load(f)
                        version = VectorClock.from_dict(data['version'])
                        self.primary_store[key] = (data['value'], version)
                        count += 1
                except Exception as e:
                    print(f"Error loading primary key {key}: {e}")
        
        return count
    
    def load_all_backups(self) -> int:
        """
        Load all backup keys from disk on startup.
        
        Returns:
            Number of keys loaded
        """
        if not self.enable_persistence or not os.path.exists(self.backup_dir):
            return 0
        
        count = 0
        for node_dir in os.listdir(self.backup_dir):
            if node_dir.startswith('node_'):
                node_id = int(node_dir.split('_')[1])
                node_backup_dir = os.path.join(self.backup_dir, node_dir)
                
                if node_id not in self.backup_store:
                    self.backup_store[node_id] = {}
                
                for filename in os.listdir(node_backup_dir):
                    if filename.endswith('.txt'):
                        key = filename[:-4]
                        key_file = os.path.join(node_backup_dir, filename)
                        try:
                            with open(key_file, 'r') as f:
                                data = json.load(f)
                                version = VectorClock.from_dict(data['version'])
                                self.backup_store[node_id][key] = (data['value'], version)
                                count += 1
                        except Exception as e:
                            print(f"Error loading backup key {key} for node {node_id}: {e}")
        
        return count
        
        return count
    
    def get_all_backups_for_node(self, for_node_id: int) -> Dict[str, Tuple[Any, VectorClock]]:
        """
        Get all backup keys for a specific primary node.
        
        Args:
            for_node_id: ID of the primary node
            
        Returns:
            Dictionary of key -> (value, version)
        """
        return self.backup_store.get(for_node_id, {}).copy()
    
    def transfer_backups_to_primary(self, for_node_id: int):
        """
        Promote backup keys to primary (used when a node fails and we take over).
        
        Args:
            for_node_id: ID of the failed node whose backups we're promoting
        """
        if for_node_id in self.backup_store:
            for key, (value, version) in self.backup_store[for_node_id].items():
                # Only promote if we don't have it or backup is newer
                if key not in self.primary_store:
                    self.primary_store[key] = (value, version)
                    if self.enable_persistence:
                        self._save_primary(key, value, version)
                else:
                    _, current_version = self.primary_store[key]
                    if version > current_version:
                        self.primary_store[key] = (value, version)
                        if self.enable_persistence:
                            self._save_primary(key, value, version)
            
            # Clear the backup after promotion
            del self.backup_store[for_node_id]
    
    # ==================== End of Persistent Storage Methods ====================
    
    def get(self, key: str) -> Optional[Tuple[Any, VectorClock]]:
        """
        Retrieve a key-value pair with its version.
        
        Args:
            key: The key to retrieve
            
        Returns:
            Tuple of (value, version) or None if key doesn't exist
        """
        return self.primary_store.get(key)
    
    def get_value(self, key: str) -> Optional[Any]:
        """
        Retrieve just the value without version metadata.
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value or None if key doesn't exist
        """
        result = self.primary_store.get(key)
        return result[0] if result else None
    
    def get_with_version(self, key: str) -> Tuple[Optional[Any], Optional[VectorClock]]:
        """
        Retrieve value and version separately.
        
        Args:
            key: The key to retrieve
            
        Returns:
            Tuple of (value, version) where either can be None if key doesn't exist
        """
        result = self.primary_store.get(key)
        if result:
            return result[0], result[1]
        return None, None
    
    def get_version(self, key: str) -> Optional[VectorClock]:
        """
        Retrieve just the version without the value.
        
        Args:
            key: The key to check
            
        Returns:
            The vector clock version or None if key doesn't exist
        """
        result = self.primary_store.get(key)
        return result[1] if result else None
    
    def delete(self, key: str) -> bool:
        """
        Delete a key-value pair from primary storage.
        
        Args:
            key: The key to delete
            
        Returns:
            True if deleted, False if key didn't exist
        """
        if key in self.primary_store:
            del self.primary_store[key]
            
            # Delete from disk if persistence enabled
            if self.enable_persistence:
                key_file = os.path.join(self.primary_dir, f"{key}.txt")
                if os.path.exists(key_file):
                    os.remove(key_file)
            
            return True
        return False
    
    def delete_backup(self, key: str, for_node_id: int) -> bool:
        """
        Delete a backup key for a specific node.
        
        Args:
            key: The key to delete
            for_node_id: ID of the primary node
            
        Returns:
            True if deleted, False if key didn't exist
        """
        if for_node_id in self.backup_store and key in self.backup_store[for_node_id]:
            del self.backup_store[for_node_id][key]
            
            # Delete from disk if persistence enabled
            if self.enable_persistence:
                node_backup_dir = os.path.join(self.backup_dir, f"node_{for_node_id}")
                key_file = os.path.join(node_backup_dir, f"{key}.txt")
                if os.path.exists(key_file):
                    os.remove(key_file)
            
            return True
        return False
    
    def get_all_primary_keys(self) -> Dict[str, Tuple[Any, VectorClock]]:
        """
        Get all primary keys with their values and versions.
        
        Returns:
            Dictionary of key -> (value, version)
        """
        return self.primary_store.copy()
    
    def get_all_backup_keys(self) -> Dict[int, Dict[str, Tuple[Any, VectorClock]]]:
        """
        Get all backup keys organized by primary node.
        
        Returns:
            Dictionary of node_id -> {key -> (value, version)}
        """
        return {node_id: keys.copy() for node_id, keys in self.backup_store.items()}
    
    def has_key(self, key: str) -> bool:
        """
        Check if a key exists in storage.
        
        Args:
            key: The key to check
            
        Returns:
            True if key exists, False otherwise
        """
        return key in self.primary_store
    
    def get_all_keys(self) -> list:
        """
        Get all keys in storage.
        
        Returns:
            List of all keys
        """
        return list(self.primary_store.keys())
    
    def get_keys_in_range(self, start: int, end: int, inclusive_start: bool = False) -> list:
        """
        Get all keys whose hash falls in the range (start, end].
        
        Args:
            start: Start of range (exclusive by default)
            end: End of range (inclusive)
            inclusive_start: If True, include start in range
            
        Returns:
            List of keys in the range
        """
        keys_in_range = []
        for key in self.primary_store.keys():
            key_hash = hash_key(key)
            if in_range(key_hash, start, end, inclusive_start):
                keys_in_range.append(key)
        return keys_in_range
    
    def transfer_keys(self, keys: list) -> Dict[str, Tuple[Any, VectorClock]]:
        """
        Transfer specified keys to another node (removes from local storage).
        
        Args:
            keys: List of keys to transfer
            
        Returns:
            Dictionary of transferred key-value-version tuples
        """
        transferred = {}
        for key in keys:
            if key in self.primary_store:
                transferred[key] = self.primary_store[key]
                del self.primary_store[key]
        return transferred
    
    def receive_keys(self, data: Dict[str, Tuple[Any, VectorClock]]):
        """
        Receive keys from another node.
        
        Args:
            data: Dictionary of key -> (value, version) tuples
        """
        for key, (value, version) in data.items():
            # Only update if we don't have the key or incoming version is newer
            if key not in self.primary_store:
                self.primary_store[key] = (value, version)
            else:
                _, current_version = self.primary_store[key]
                if version > current_version:
                    self.primary_store[key] = (value, version)
    
    def size(self) -> int:
        """Get the number of keys in storage."""
        return len(self.primary_store)
    
    def clear(self):
        """Clear all data from storage."""
        self.primary_store.clear()
    
    def __repr__(self) -> str:
        return f"ChordStorage(node={self.node_id}, keys={self.size()})"


def hash_key(key: str, m: int = None) -> int:
    """
    Hash a key to an identifier in the Chord ring.
    
    Args:
        key: The key to hash
        m: Bit size of identifier space (from config if None)
        
    Returns:
        Integer identifier in range [0, 2^m)
    """
    if m is None:
        from config import M
        m = M
    
    # Use SHA-1 hash
    hash_obj = hashlib.sha1(key.encode())
    hash_bytes = hash_obj.digest()
    # Convert to integer and mod by 2^m
    hash_int = int.from_bytes(hash_bytes, byteorder='big')
    return hash_int % (2 ** m)


def in_range(identifier: int, start: int, end: int, inclusive_start: bool = False) -> bool:
    """
    Check if identifier is in the range (start, end] on the circular identifier space.
    
    Args:
        identifier: The identifier to check
        start: Start of range
        end: End of range
        inclusive_start: If True, range is [start, end] instead of (start, end]
        
    Returns:
        True if identifier is in range, False otherwise
    """
    if start == end:
        # Full circle
        return True
    elif start < end:
        # Normal range
        if inclusive_start:
            return start <= identifier <= end
        else:
            return start < identifier <= end
    else:
        # Wraparound range
        if inclusive_start:
            return identifier >= start or identifier <= end
        else:
            return identifier > start or identifier <= end


if __name__ == "__main__":
    # Simple test
    storage = ChordStorage(node_id=1)
    
    # Test put/get
    v1 = storage.put("key1", "value1")
    print(f"Stored key1 with version: {v1}")
    
    result = storage.get("key1")
    print(f"Retrieved: {result}")
    
    # Test hash_key
    print(f"\nKey hashing:")
    for key in ["key1", "key2", "key3"]:
        print(f"  {key} -> {hash_key(key)}")
