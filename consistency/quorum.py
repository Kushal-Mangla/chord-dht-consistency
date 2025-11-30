"""
Quorum-based read/write operations for tunable consistency.
"""

import asyncio
import logging
from typing import List, Optional, Tuple, Any
from chord.routing import NodeInfo
from consistency.vector_clock import VectorClock, get_latest_version
from consistency.replication import ReplicationManager


class QuorumManager:
    """
    Manages quorum-based reads and writes with tunable consistency.
    
    Implements:
    - Write quorum (W): Minimum acknowledgments for successful write
    - Read quorum (R): Minimum replicas to read from
    - Read repair: Automatically fix stale replicas
    
    When R + W > N: Strong consistency (reads see latest writes)
    When R + W ≤ N: Eventual consistency (better performance)
    """
    
    def __init__(self, 
                 node_id: int,
                 n_replicas: int = 3,
                 read_quorum: int = 2,
                 write_quorum: int = 2,
                 enable_read_repair: bool = True,
                 network_manager=None):
        """
        Initialize quorum manager.
        
        Args:
            node_id: ID of the node owning this manager
            n_replicas: Total number of replicas (N)
            read_quorum: Read quorum size (R)
            write_quorum: Write quorum size (W)
            enable_read_repair: Enable automatic read repair
            network_manager: NetworkManager for communication
        """
        self.node_id = node_id
        self.n_replicas = n_replicas
        self.read_quorum = read_quorum
        self.write_quorum = write_quorum
        self.enable_read_repair = enable_read_repair
        
        # Validate quorum settings
        self._validate_quorum()
        
        # Replication manager
        self.replication_mgr = ReplicationManager(node_id, n_replicas, network_manager)
        
        self.logger = logging.getLogger(f"QuorumMgr-{node_id}")
        self.logger.info(f"Quorum: N={n_replicas}, R={read_quorum}, W={write_quorum}")
        
        # Log consistency level
        if read_quorum + write_quorum > n_replicas:
            self.logger.info("Consistency: STRONG (R+W > N)")
        elif read_quorum + write_quorum == n_replicas:
            self.logger.info("Consistency: MODERATE (R+W = N)")
        else:
            self.logger.info("Consistency: EVENTUAL (R+W < N)")
    
    def _validate_quorum(self):
        """Validate quorum configuration."""
        if self.read_quorum < 1 or self.read_quorum > self.n_replicas:
            raise ValueError(f"Invalid read quorum: {self.read_quorum} "
                           f"(must be 1 ≤ R ≤ {self.n_replicas})")
        
        if self.write_quorum < 1 or self.write_quorum > self.n_replicas:
            raise ValueError(f"Invalid write quorum: {self.write_quorum} "
                           f"(must be 1 ≤ W ≤ {self.n_replicas})")
    
    async def quorum_put(self,
                         key: str,
                         value: Any,
                         version: VectorClock,
                         replicas: List[NodeInfo],
                         timeout: float = 2.0,
                         primary_node_id: int = None) -> Tuple[bool, VectorClock]:
        """
        Perform a quorum write operation.
        
        Args:
            key: Key to write
            value: Value to write
            version: Vector clock version
            replicas: List of replica nodes
            timeout: Timeout in seconds
            primary_node_id: ID of primary node (for sloppy quorum)
            
        Returns:
            Tuple of (success, version)
        """
        if not replicas:
            self.logger.error("No replicas available for write")
            return False, version
        
        self.logger.info(f"Quorum PUT: {key}={value} (need {self.write_quorum} acks)")
        
        try:
            # Replicate to all nodes with timeout
            acknowledged = await asyncio.wait_for(
                self.replication_mgr.replicate_put(key, value, version, replicas, primary_node_id),
                timeout=timeout
            )
            
            num_acks = len(acknowledged)
            success = num_acks >= self.write_quorum
            
            if success:
                self.logger.info(f"Quorum PUT succeeded: {num_acks}/{len(replicas)} acks")
            else:
                self.logger.warning(f"Quorum PUT failed: {num_acks}/{len(replicas)} acks "
                                  f"(needed {self.write_quorum})")
            
            return success, version
            
        except asyncio.TimeoutError:
            self.logger.error(f"Quorum PUT timeout after {timeout}s")
            return False, version
    
    async def quorum_get(self,
                        key: str,
                        replicas: List[NodeInfo],
                        timeout: float = 2.0,
                        primary_node_id: int = None) -> Optional[Tuple[Any, VectorClock]]:
        """
        Perform a quorum read operation.
        
        Args:
            key: Key to read
            replicas: List of replica nodes
            timeout: Timeout in seconds
            primary_node_id: Hint about which node is primary (for checking backups)
            
        Returns:
            Tuple of (value, version) or None if quorum not met
        """
        if not replicas:
            self.logger.error("No replicas available for read")
            return None
        
        self.logger.info(f"Quorum GET: {key} (need {self.read_quorum} responses)")
        
        try:
            # Read from replicas with timeout
            reads = await asyncio.wait_for(
                self.replication_mgr.replicate_get(key, replicas, primary_node_id),
                timeout=timeout
            )
            
            num_reads = len(reads)
            
            if num_reads < self.read_quorum:
                self.logger.warning(f"Quorum GET failed: {num_reads}/{len(replicas)} "
                                  f"responses (needed {self.read_quorum})")
                return None
            
            self.logger.info(f"Quorum GET: {num_reads}/{len(replicas)} responses")
            
            # Find the latest version
            versions = [version for _, _, version in reads]
            latest_version = get_latest_version(versions)
            
            if latest_version is None:
                # Concurrent versions (conflict)
                self.logger.warning(f"Conflict detected for {key}: "
                                  f"{len(versions)} concurrent versions")
                # For now, pick arbitrarily (could implement custom resolution)
                latest_version = versions[0]
            
            # Get value with latest version
            latest_value = None
            stale_replicas = []
            
            for node, value, version in reads:
                if version == latest_version:
                    latest_value = value
                else:
                    # This replica is stale
                    stale_replicas.append(node)
            
            # Perform read repair if enabled
            if self.enable_read_repair and stale_replicas:
                self.logger.info(f"Read repair: {len(stale_replicas)} stale replicas")
                asyncio.create_task(
                    self.replication_mgr.repair_replicas(
                        key, latest_value, latest_version, stale_replicas
                    )
                )
            
            return latest_value, latest_version
            
        except asyncio.TimeoutError:
            self.logger.error(f"Quorum GET timeout after {timeout}s")
            return None
    
    def get_consistency_level(self) -> str:
        """
        Get the consistency level based on quorum settings.
        
        Returns:
            String describing consistency level
        """
        r, w, n = self.read_quorum, self.write_quorum, self.n_replicas
        
        if r + w > n:
            return "STRONG"
        elif r + w == n:
            return "MODERATE"
        else:
            return "EVENTUAL"
    
    def update_quorum_config(self, read_quorum: int = None, write_quorum: int = None):
        """
        Update quorum configuration.
        
        Args:
            read_quorum: New read quorum (None to keep current)
            write_quorum: New write quorum (None to keep current)
        """
        if read_quorum is not None:
            self.read_quorum = read_quorum
        
        if write_quorum is not None:
            self.write_quorum = write_quorum
        
        # Validate new configuration
        self._validate_quorum()
        
        self.logger.info(f"Updated quorum: N={self.n_replicas}, "
                        f"R={self.read_quorum}, W={self.write_quorum}")
        self.logger.info(f"Consistency: {self.get_consistency_level()}")
    
    def __repr__(self) -> str:
        return (f"QuorumManager(node={self.node_id}, N={self.n_replicas}, "
                f"R={self.read_quorum}, W={self.write_quorum})")


if __name__ == "__main__":
    # Test quorum manager
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    async def test():
        # Create manager with strong consistency (R+W > N)
        mgr = QuorumManager(
            node_id=1,
            n_replicas=3,
            read_quorum=2,
            write_quorum=2,
            enable_read_repair=True
        )
        
        print(f"Consistency level: {mgr.get_consistency_level()}\n")
        
        # Create dummy replicas
        replicas = [
            NodeInfo(2, "localhost:5001"),
            NodeInfo(3, "localhost:5002"),
            NodeInfo(4, "localhost:5003"),
        ]
        
        # Test quorum write
        version = VectorClock({1: 1})
        success, final_version = await mgr.quorum_put(
            "key1", "value1", version, replicas
        )
        print(f"\nQuorum PUT: success={success}, version={final_version}")
        
        # Test quorum read
        result = await mgr.quorum_get("key1", replicas)
        if result:
            value, version = result
            print(f"\nQuorum GET: value={value}, version={version}")
        else:
            print("\nQuorum GET: failed")
        
        # Test different consistency levels
        print("\n" + "="*50)
        print("Testing different consistency levels:")
        print("="*50)
        
        for r, w in [(1, 1), (2, 2), (3, 3), (1, 3), (3, 1)]:
            mgr.update_quorum_config(read_quorum=r, write_quorum=w)
            print(f"R={r}, W={w}, N=3 -> {mgr.get_consistency_level()}")
    
    asyncio.run(test())
