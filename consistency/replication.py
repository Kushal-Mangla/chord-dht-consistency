"""
Replication manager for N-way replication in Chord.
"""

import asyncio
import logging
from typing import List, Dict, Optional, Tuple, Any, TYPE_CHECKING
from chord.routing import NodeInfo
from consistency.vector_clock import VectorClock

if TYPE_CHECKING:
    from communication.network import NetworkManager


class ReplicationManager:
    """
    Manages N-way replication across Chord successor nodes.
    
    Coordinates replicating data to N successor nodes and
    maintaining consistency across replicas.
    """
    
    def __init__(self, node_id: int, n_replicas: int = 3, network_manager=None):
        """
        Initialize replication manager.
        
        Args:
            node_id: ID of the node owning this manager
            n_replicas: Number of replicas to maintain
            network_manager: NetworkManager instance for sending messages
        """
        self.node_id = node_id
        self.n_replicas = n_replicas
        self.network_manager = network_manager
        self.logger = logging.getLogger(f"ReplicationMgr-{node_id}")
    
    async def replicate_put(self, 
                           key: str, 
                           value: Any, 
                           version: VectorClock,
                           replicas: List[NodeInfo],
                           primary_node_id: int = None) -> List[NodeInfo]:
        """
        Replicate a PUT operation to successor nodes.
        
        Args:
            key: The key to replicate
            value: The value to replicate
            version: Vector clock version
            replicas: List of replica nodes
            primary_node_id: ID of the primary/responsible node (for sloppy quorum)
            
        Returns:
            List of nodes that acknowledged the replication
        """
        if not replicas:
            return []
        
        self.logger.info(f"Replicating {key} to {len(replicas)} nodes")
        
        # Send replication requests to all replicas
        tasks = []
        for replica in replicas:
            task = self._send_put_replica(replica, key, value, version, primary_node_id)
            tasks.append(task)
        
        # Wait for all responses
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Count successful acknowledgments
        acknowledged = []
        for replica, result in zip(replicas, results):
            if isinstance(result, Exception):
                self.logger.warning(f"Replica {replica} failed: {result}")
            elif result:
                acknowledged.append(replica)
                self.logger.debug(f"Replica {replica} acknowledged")
        
        return acknowledged
    
    async def _send_put_replica(self, 
                                node: NodeInfo, 
                                key: str, 
                                value: Any,
                                version: VectorClock,
                                primary_node_id: int = None) -> bool:
        """
        Send PUT_REPLICA message to a node.
        
        Args:
            node: Target node
            key: Key to replicate
            value: Value to replicate
            version: Vector clock version
            primary_node_id: ID of primary node (for sloppy quorum/hinted handoff)
            
        Returns:
            True if acknowledged, False otherwise
        """
        if not self.network_manager:
            # No network manager, simulate for testing
            await asyncio.sleep(0.01)
            import random
            return random.random() < 0.9
        
        try:
            from communication.message import Message, MessageType
            
            # Create PUT_REPLICA message
            msg = Message(
                msg_id=f"put_replica_{id(self)}_{asyncio.get_event_loop().time()}",
                msg_type=MessageType.PUT_REPLICA,
                sender_id=self.node_id,
                sender_address=self.network_manager.address,
                data={
                    'key': key,
                    'value': value,
                    'version': version.to_dict(),
                    'primary_node_id': primary_node_id if primary_node_id is not None else self.node_id
                }
            )
            
            # Send with timeout
            response = await self.network_manager.send_message(
                node.address,
                msg,
                wait_response=True,
                timeout=2.0
            )
            
            if response and response.msg_type == MessageType.PUT_REPLICA_REPLY:
                success = response.data.get('status') == 'ok'
                if success:
                    self.logger.debug(f"PUT_REPLICA to {node.address}: {key}={value}")
                else:
                    self.logger.warning(f"PUT_REPLICA to {node.address} failed")
                return success
            else:
                self.logger.warning(f"PUT_REPLICA to {node.address} no response")
                return False
                
        except Exception as e:
            self.logger.error(f"PUT_REPLICA to {node.address} error: {e}")
            return False
    
    async def replicate_get(self,
                           key: str,
                           replicas: List[NodeInfo],
                           primary_node_id: int = None) -> List[Tuple[NodeInfo, Any, VectorClock]]:
        """
        Read from multiple replicas.
        
        Args:
            key: The key to read
            replicas: List of replica nodes to query
            primary_node_id: Hint about which node is primary (for checking backups)
            
        Returns:
            List of tuples (node, value, version) from successful reads
        """
        if not replicas:
            return []
        
        self.logger.info(f"Reading {key} from {len(replicas)} replicas")
        
        # Send GET requests to all replicas
        tasks = []
        for replica in replicas:
            task = self._send_get_replica(replica, key, primary_node_id)
            tasks.append(task)
        
        # Wait for all responses
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect successful reads
        reads = []
        for replica, result in zip(replicas, results):
            if isinstance(result, Exception):
                self.logger.warning(f"Replica {replica} read failed: {result}")
            elif result is not None:
                value, version = result
                reads.append((replica, value, version))
                self.logger.debug(f"Replica {replica} returned version {version}")
        
        return reads
    
    async def _send_get_replica(self, 
                               node: NodeInfo, 
                               key: str,
                               primary_node_id: int = None) -> Optional[Tuple[Any, VectorClock]]:
        """
        Send GET_REPLICA message to a node.
        
        Args:
            node: Target node
            key: Key to read
            primary_node_id: Hint about which node is primary (for checking backups)
            
        Returns:
            Tuple of (value, version) or None if failed
        """
        if not self.network_manager:
            # No network manager, simulate for testing
            await asyncio.sleep(0.01)
            import random
            if random.random() < 0.9:
                dummy_version = VectorClock({self.node_id: 1})
                dummy_value = f"value_for_{key}"
                return (dummy_value, dummy_version)
            return None
        
        try:
            from communication.message import Message, MessageType
            
            # Create GET_REPLICA message
            msg_data = {'key': key}
            if primary_node_id is not None:
                msg_data['primary_node_id'] = primary_node_id
            
            msg = Message(
                msg_id=f"get_replica_{id(self)}_{asyncio.get_event_loop().time()}",
                msg_type=MessageType.GET_REPLICA,
                sender_id=self.node_id,
                sender_address=self.network_manager.address,
                data=msg_data
            )
            
            # Send with timeout
            response = await self.network_manager.send_message(
                node.address,
                msg,
                wait_response=True,
                timeout=2.0
            )
            
            if response and response.msg_type == MessageType.GET_REPLICA_REPLY:
                value = response.data.get('value')
                version_dict = response.data.get('version')
                
                if value is not None and version_dict:
                    version = VectorClock.from_dict(version_dict)
                    self.logger.debug(f"GET_REPLICA from {node.address}: {key}={value}")
                    return (value, version)
                else:
                    self.logger.debug(f"GET_REPLICA from {node.address}: key not found")
                    return None
            else:
                self.logger.warning(f"GET_REPLICA from {node.address} no response")
                return None
                
        except Exception as e:
            self.logger.error(f"GET_REPLICA from {node.address} error: {e}")
            return None
    
    async def repair_replicas(self,
                             key: str,
                             latest_value: Any,
                             latest_version: VectorClock,
                             stale_replicas: List[NodeInfo]):
        """
        Perform read-repair on stale replicas.
        
        Args:
            key: The key to repair
            latest_value: The most recent value
            latest_version: The most recent version
            stale_replicas: List of replicas with stale data
        """
        if not stale_replicas:
            return
        
        self.logger.info(f"Read-repair: updating {len(stale_replicas)} stale replicas")
        
        # Send repair updates to stale replicas
        tasks = []
        for replica in stale_replicas:
            task = self._send_put_replica(replica, key, latest_value, latest_version)
            tasks.append(task)
        
        # Wait for all repairs (fire-and-forget)
        await asyncio.gather(*tasks, return_exceptions=True)
        self.logger.info(f"Read-repair completed for {key}")
    
    def select_replicas(self, 
                       successor_list: List[NodeInfo], 
                       n: Optional[int] = None) -> List[NodeInfo]:
        """
        Select n replicas from successor list.
        
        Args:
            successor_list: Available successor nodes
            n: Number of replicas (default: self.n_replicas)
            
        Returns:
            List of selected replica nodes
        """
        if n is None:
            n = self.n_replicas
        
        # Take first n successors
        return successor_list[:min(n, len(successor_list))]
    
    def __repr__(self) -> str:
        return f"ReplicationManager(node={self.node_id}, N={self.n_replicas})"


if __name__ == "__main__":
    # Test replication manager
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    async def test():
        # Create manager
        mgr = ReplicationManager(node_id=1, n_replicas=3)
        
        # Create dummy replicas
        replicas = [
            NodeInfo(2, "localhost:5001"),
            NodeInfo(3, "localhost:5002"),
            NodeInfo(4, "localhost:5003"),
        ]
        
        # Test replication
        version = VectorClock({1: 1})
        acks = await mgr.replicate_put("key1", "value1", version, replicas)
        print(f"\nAcknowledged by {len(acks)} replicas: {acks}")
        
        # Test reading
        reads = await mgr.replicate_get("key1", replicas)
        print(f"\nRead from {len(reads)} replicas:")
        for node, value, ver in reads:
            print(f"  {node}: {value} (version: {ver})")
    
    asyncio.run(test())
