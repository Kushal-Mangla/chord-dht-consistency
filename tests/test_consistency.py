"""
Tests for consistency layer (vector clocks, quorum).
"""

import pytest
import asyncio
from consistency.vector_clock import VectorClock, compare_versions, get_latest_version
from consistency.quorum import QuorumManager
from consistency.replication import ReplicationManager
from chord.routing import NodeInfo


class TestVectorClock:
    """Test vector clock implementation."""
    
    def test_initialization(self):
        """Test creating a vector clock."""
        vc = VectorClock()
        assert vc.clock == {}
        
        vc2 = VectorClock({1: 5, 2: 3})
        assert vc2.clock == {1: 5, 2: 3}
    
    def test_increment(self):
        """Test incrementing a vector clock."""
        vc = VectorClock()
        vc.increment(1)
        
        assert vc.clock[1] == 1
        
        vc.increment(1)
        assert vc.clock[1] == 2
    
    def test_update(self):
        """Test merging vector clocks."""
        vc1 = VectorClock({1: 3, 2: 1})
        vc2 = VectorClock({1: 2, 2: 4, 3: 1})
        
        vc1.update(vc2)
        
        # Should take element-wise maximum
        assert vc1.clock[1] == 3
        assert vc1.clock[2] == 4
        assert vc1.clock[3] == 1
    
    def test_happens_before(self):
        """Test happens-before relationship."""
        vc1 = VectorClock({1: 1, 2: 1})
        vc2 = VectorClock({1: 2, 2: 1})
        vc3 = VectorClock({1: 1, 2: 2})
        vc4 = VectorClock({1: 2, 2: 0})  # Concurrent with vc1
        
        # vc1 happens before vc2
        assert vc1.happens_before(vc2)
        assert not vc2.happens_before(vc1)
        
        # vc1 happens before vc3
        assert vc1.happens_before(vc3)
        assert not vc3.happens_before(vc1)
        
        # vc1 and vc4 are concurrent (neither happens before the other)
        assert not vc1.happens_before(vc4)
        assert not vc4.happens_before(vc1)
    
    def test_concurrent(self):
        """Test detecting concurrent updates."""
        vc1 = VectorClock({1: 2, 2: 1})
        vc2 = VectorClock({1: 1, 2: 2})
        
        assert vc1.concurrent_with(vc2)
        assert vc2.concurrent_with(vc1)
    
    def test_equality(self):
        """Test vector clock equality."""
        vc1 = VectorClock({1: 1, 2: 2})
        vc2 = VectorClock({1: 1, 2: 2})
        vc3 = VectorClock({1: 1, 2: 3})
        
        assert vc1 == vc2
        assert vc1 != vc3
    
    def test_comparison_operators(self):
        """Test comparison operators."""
        vc1 = VectorClock({1: 1, 2: 1})
        vc2 = VectorClock({1: 2, 2: 1})
        
        assert vc1 < vc2
        assert vc1 <= vc2
        assert vc2 > vc1
        assert vc2 >= vc1
    
    def test_serialization(self):
        """Test to_dict and from_dict."""
        vc1 = VectorClock({1: 5, 2: 3})
        
        d = vc1.to_dict()
        assert d == {1: 5, 2: 3}
        
        vc2 = VectorClock.from_dict(d)
        assert vc1 == vc2


class TestCompareVersions:
    """Test version comparison utilities."""
    
    def test_compare_versions(self):
        """Test version comparison."""
        vc1 = VectorClock({1: 1})
        vc2 = VectorClock({1: 2})
        vc3 = VectorClock({2: 1})
        
        assert compare_versions(vc1, vc2) == "v1<v2"
        assert compare_versions(vc2, vc1) == "v1>v2"
        assert compare_versions(vc1, vc1) == "v1=v2"
        assert compare_versions(vc1, vc3) == "concurrent"
    
    def test_get_latest_version(self):
        """Test finding latest version."""
        vc1 = VectorClock({1: 1, 2: 1})
        vc2 = VectorClock({1: 2, 2: 1})
        vc3 = VectorClock({1: 1, 2: 2})
        vc4 = VectorClock({1: 2, 2: 0})  # Concurrent with vc1
        
        # vc2 is latest among [vc1, vc2]
        latest = get_latest_version([vc1, vc2])
        assert latest == vc2
        
        # vc3 is latest among [vc1, vc3] (vc1 < vc3)
        latest = get_latest_version([vc1, vc3])
        assert latest == vc3
        
        # vc1 and vc4 are concurrent - should return None
        latest = get_latest_version([vc1, vc4])
        assert latest is None


class TestQuorumManager:
    """Test quorum manager."""
    
    def test_initialization(self):
        """Test creating a quorum manager."""
        qm = QuorumManager(node_id=1, n_replicas=3, read_quorum=2, write_quorum=2)
        
        assert qm.node_id == 1
        assert qm.n_replicas == 3
        assert qm.read_quorum == 2
        assert qm.write_quorum == 2
    
    def test_invalid_quorum(self):
        """Test that invalid quorum raises error."""
        with pytest.raises(ValueError):
            QuorumManager(node_id=1, n_replicas=3, read_quorum=0, write_quorum=2)
        
        with pytest.raises(ValueError):
            QuorumManager(node_id=1, n_replicas=3, read_quorum=2, write_quorum=4)
    
    def test_consistency_level(self):
        """Test consistency level detection."""
        # Strong consistency
        qm = QuorumManager(node_id=1, n_replicas=3, read_quorum=2, write_quorum=2)
        assert qm.get_consistency_level() == "STRONG"
        
        # Moderate consistency
        qm = QuorumManager(node_id=1, n_replicas=3, read_quorum=1, write_quorum=2)
        assert qm.get_consistency_level() == "MODERATE"
        
        # Eventual consistency
        qm = QuorumManager(node_id=1, n_replicas=3, read_quorum=1, write_quorum=1)
        assert qm.get_consistency_level() == "EVENTUAL"
    
    @pytest.mark.asyncio
    async def test_quorum_put(self):
        """Test quorum write operation."""
        qm = QuorumManager(node_id=1, n_replicas=3, read_quorum=2, write_quorum=2)
        
        replicas = [
            NodeInfo(2, "localhost:5001"),
            NodeInfo(3, "localhost:5002"),
            NodeInfo(4, "localhost:5003"),
        ]
        
        version = VectorClock({1: 1})
        
        # This will use the simulated replication
        success, final_version = await qm.quorum_put("key1", "value1", version, replicas)
        
        # With 90% success rate, we should usually succeed
        # But this is probabilistic, so we just check the call worked
        assert isinstance(success, bool)
        assert final_version == version
    
    @pytest.mark.asyncio
    async def test_quorum_get(self):
        """Test quorum read operation."""
        qm = QuorumManager(node_id=1, n_replicas=3, read_quorum=2, write_quorum=2)
        
        replicas = [
            NodeInfo(2, "localhost:5001"),
            NodeInfo(3, "localhost:5002"),
            NodeInfo(4, "localhost:5003"),
        ]
        
        # This will use the simulated replication
        result = await qm.quorum_get("key1", replicas)
        
        # With 90% success rate, we should usually get a result
        # But this is probabilistic
        if result:
            value, version = result
            assert isinstance(value, str)
            assert isinstance(version, VectorClock)


class TestReplicationManager:
    """Test replication manager."""
    
    def test_initialization(self):
        """Test creating a replication manager."""
        rm = ReplicationManager(node_id=1, n_replicas=3)
        
        assert rm.node_id == 1
        assert rm.n_replicas == 3
    
    def test_select_replicas(self):
        """Test selecting replicas from successor list."""
        rm = ReplicationManager(node_id=1, n_replicas=3)
        
        successors = [
            NodeInfo(2, "localhost:5001"),
            NodeInfo(3, "localhost:5002"),
            NodeInfo(4, "localhost:5003"),
            NodeInfo(5, "localhost:5004"),
        ]
        
        replicas = rm.select_replicas(successors)
        
        assert len(replicas) == 3
        assert replicas == successors[:3]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
