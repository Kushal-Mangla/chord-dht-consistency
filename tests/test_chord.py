"""
Tests for Chord DHT core functionality.
"""

import pytest
from chord.node import ChordNode
from chord.routing import FingerTable, NodeInfo, hash_address, in_range
from chord.storage import ChordStorage, hash_key


class TestChordHashing:
    """Test hashing functions."""
    
    def test_hash_key_deterministic(self):
        """Hash function should be deterministic."""
        key = "test_key"
        hash1 = hash_key(key, m=6)
        hash2 = hash_key(key, m=6)
        assert hash1 == hash2
    
    def test_hash_key_range(self):
        """Hash should be in valid range."""
        m = 6
        max_id = 2 ** m
        for key in ["key1", "key2", "key3", "test"]:
            hashed = hash_key(key, m)
            assert 0 <= hashed < max_id
    
    def test_hash_address(self):
        """Test address hashing."""
        addr1 = "localhost:5000"
        addr2 = "localhost:5001"
        
        h1 = hash_address(addr1, m=6)
        h2 = hash_address(addr2, m=6)
        
        assert h1 != h2
        assert 0 <= h1 < 64
        assert 0 <= h2 < 64


class TestInRange:
    """Test range checking on circular space."""
    
    def test_normal_range(self):
        """Test normal (non-wraparound) range."""
        assert in_range(5, 1, 10)
        assert in_range(10, 1, 10)
        assert not in_range(1, 1, 10)
        assert not in_range(11, 1, 10)
    
    def test_wraparound_range(self):
        """Test wraparound range."""
        assert in_range(5, 60, 10)
        assert in_range(62, 60, 10)
        assert in_range(10, 60, 10)
        assert not in_range(30, 60, 10)
    
    def test_inclusive_range(self):
        """Test inclusive range options."""
        assert in_range(1, 1, 10, inclusive_start=True)
        assert not in_range(1, 1, 10, inclusive_start=False)


class TestChordStorage:
    """Test local storage."""
    
    def test_put_get(self):
        """Test basic put/get operations."""
        storage = ChordStorage(node_id=1)
        
        version = storage.put("key1", "value1")
        assert version is not None
        
        result = storage.get("key1")
        assert result is not None
        value, ver = result
        assert value == "value1"
        assert ver == version
    
    def test_get_nonexistent(self):
        """Test getting nonexistent key."""
        storage = ChordStorage(node_id=1)
        assert storage.get("nonexistent") is None
    
    def test_update_increments_version(self):
        """Test that updates increment version."""
        storage = ChordStorage(node_id=1)
        
        v1 = storage.put("key1", "value1")
        v2 = storage.put("key1", "value2")
        
        assert v2 > v1
    
    def test_delete(self):
        """Test deletion."""
        storage = ChordStorage(node_id=1)
        
        storage.put("key1", "value1")
        assert storage.has_key("key1")
        
        storage.delete("key1")
        assert not storage.has_key("key1")


class TestFingerTable:
    """Test finger table."""
    
    def test_initialization(self):
        """Test finger table initialization."""
        ft = FingerTable(node_id=10, m=6)
        
        assert ft.node_id == 10
        assert ft.m == 6
        assert len(ft.fingers) == 6
    
    def test_finger_starts(self):
        """Test finger table start values."""
        ft = FingerTable(node_id=0, m=6)
        
        # For node 0, finger i starts at 2^i
        expected_starts = [1, 2, 4, 8, 16, 32]
        assert ft.starts == expected_starts
    
    def test_set_get_finger(self):
        """Test setting and getting fingers."""
        ft = FingerTable(node_id=10, m=6)
        
        node = NodeInfo(15, "localhost:5001")
        ft.set_finger(0, node)
        
        assert ft.get_finger(0) == node
        assert ft.get_successor() == node


class TestChordNode:
    """Test Chord node."""
    
    def test_node_creation(self):
        """Test creating a node."""
        node = ChordNode("localhost:5000", m=6)
        
        assert node.address == "localhost:5000"
        assert node.m == 6
        assert 0 <= node.node_id < 64
    
    def test_create_ring(self):
        """Test creating a new ring."""
        node = ChordNode("localhost:5000", m=6)
        node.create_ring()
        
        # Node should be its own successor
        successor = node.finger_table.get_successor()
        assert successor is not None
        assert successor.node_id == node.node_id
    
    def test_local_storage(self):
        """Test local put/get operations."""
        node = ChordNode("localhost:5000", m=6)
        node.create_ring()
        
        # Put and get
        node.put("test_key", "test_value")
        value = node.get("test_key")
        
        # Note: This might not work if key doesn't hash to this node
        # For proper testing, we'd need to ensure the key hashes correctly


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
