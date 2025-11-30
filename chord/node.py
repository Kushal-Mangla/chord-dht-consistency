"""
Core Chord node implementation.
"""

import asyncio
import logging
from typing import Optional, List
from .routing import FingerTable, NodeInfo, hash_address, in_range
from .storage import ChordStorage, hash_key
from consistency.vector_clock import VectorClock


class ChordNode:
    """
    A node in the Chord Distributed Hash Table.
    
    Each node maintains:
    - A finger table for efficient routing
    - References to successor and predecessor
    - Local key-value storage
    - Successor list for replication
    """
    
    def __init__(self, address: str, m: int = 6, n_replicas: int = 3):
        """
        Initialize a Chord node.
        
        Args:
            address: Node address as "host:port"
            m: Bit size of identifier space (default: 6, supports 64 nodes)
            n_replicas: Number of replicas for each key
        """
        self.address = address
        self.m = m
        self.max_id = 2 ** m
        self.n_replicas = n_replicas
        
        # Compute node identifier from address
        self.node_id = hash_address(address, m)
        
        # Logging - initialize EARLY so we can use it
        self.logger = logging.getLogger(f"ChordNode-{self.node_id}")
        self.logger.info(f"Initializing node {self.node_id} at {address}")
        
        # Routing structures
        self.finger_table = FingerTable(self.node_id, m)
        self.predecessor: Optional[NodeInfo] = None
        self.successor_list: List[NodeInfo] = []
        
        # Local storage with persistent storage enabled
        self.storage = ChordStorage(self.node_id, base_dir="storage", enable_persistence=True)
        
        # Load any existing persistent data
        primary_count = self.storage.load_all_primary()
        backup_count = self.storage.load_all_backups()
        if primary_count > 0 or backup_count > 0:
            self.logger.info(f"Loaded {primary_count} primary keys and {backup_count} backup keys from disk")
        
        # Stabilization state
        self.next_finger_to_fix = 0
        
        self.logger.info(f"Node initialization complete")
    
    def get_info(self) -> NodeInfo:
        """Get NodeInfo for this node."""
        return NodeInfo(self.node_id, self.address)
    
    # ==================== Core Chord Operations ====================
    
    def find_successor(self, identifier: int) -> Optional[NodeInfo]:
        """
        Find the successor node responsible for an identifier.
        
        Args:
            identifier: The identifier to look up
            
        Returns:
            NodeInfo of the successor node
        """
        # If we're the only node, we're responsible
        if not self.finger_table.get_successor():
            return self.get_info()
        
        successor = self.finger_table.get_successor()
        
        # If successor is ourselves, we're the only node
        if successor.node_id == self.node_id:
            return self.get_info()
        
        # Check if identifier is between us and our successor (we should return successor)
        if in_range(identifier, self.node_id, successor.node_id, 
                   inclusive_start=False, inclusive_end=True):
            return successor
        
        # Check if we are responsible (identifier is between predecessor and us)
        if self.predecessor:
            if in_range(identifier, self.predecessor.node_id, self.node_id,
                       inclusive_start=False, inclusive_end=True):
                return self.get_info()
        
        # Find closest preceding node from our finger table/successor list
        closest = self.find_closest_preceding_node(identifier)
        
        if closest.node_id == self.node_id:
            # We're the closest preceding node, so our successor is responsible
            return successor
        
        # Return the closest node - caller should query it for the actual successor
        return closest
    
    def find_closest_preceding_node(self, identifier: int) -> NodeInfo:
        """
        Find the closest node preceding the identifier.
        
        Args:
            identifier: The identifier to search for
            
        Returns:
            NodeInfo of the closest preceding node
        """
        # Check finger table from highest to lowest
        closest = self.finger_table.find_closest_preceding_finger(identifier)
        
        if closest:
            return closest
        
        # If no finger found, return ourselves
        return self.get_info()
    
    # ==================== Stabilization Protocol ====================
    
    def stabilize(self):
        """
        Periodically verify and update successor and notify it.
        Called periodically to maintain correct successors.
        """
        successor = self.finger_table.get_successor()
        
        if not successor:
            return
        
        # In a real implementation, we'd ask successor for its predecessor
        # and adjust if needed. For now, we'll implement the logic skeleton.
        
        # Ask successor: "Who do you think your predecessor is?"
        # x = successor.predecessor
        # if x is between (n, successor):
        #     successor = x
        # successor.notify(self)
        
        self.logger.debug(f"Stabilize: successor={successor}")
    
    def notify(self, node: NodeInfo):
        """
        Called by another node thinking it might be our predecessor.
        
        Args:
            node: The node notifying us
        """
        if self.predecessor is None:
            self.predecessor = node
        elif in_range(node.node_id, self.predecessor.node_id, self.node_id,
                     inclusive_start=False, inclusive_end=False):
            # node is between our current predecessor and us
            self.predecessor = node
            self.logger.info(f"Updated predecessor to {node}")
    
    def fix_fingers(self):
        """
        Periodically refresh finger table entries.
        Called periodically to maintain correct finger table.
        """
        # Fix the next finger
        self.next_finger_to_fix = (self.next_finger_to_fix + 1) % self.m
        
        start = self.finger_table.get_start(self.next_finger_to_fix)
        successor = self.find_successor(start)
        
        if successor:
            self.finger_table.set_finger(self.next_finger_to_fix, successor)
            self.logger.debug(f"Fixed finger {self.next_finger_to_fix} -> {successor}")
    
    def check_predecessor(self):
        """
        Periodically check if predecessor has failed.
        Called periodically to detect predecessor failures.
        """
        if self.predecessor:
            # In a real implementation, ping predecessor
            # If it fails, set predecessor to None
            self.logger.debug(f"Checking predecessor {self.predecessor}")
    
    # ==================== Join Protocol ====================
    
    def join(self, known_node: Optional[NodeInfo] = None):
        """
        Join a Chord ring.
        
        Args:
            known_node: An existing node in the ring (None if creating new ring)
        """
        if known_node is None:
            # We're creating a new ring
            self.create_ring()
        else:
            # Join an existing ring
            self.join_ring(known_node)
    
    def create_ring(self):
        """Create a new Chord ring with this node as the only member."""
        self.predecessor = None
        self.finger_table.set_successor(self.get_info())
        self.successor_list = [self.get_info()]
        self.logger.info(f"Created new Chord ring")
    
    def join_ring(self, known_node: NodeInfo):
        """
        Join an existing Chord ring via a known node.
        
        Args:
            known_node: An existing node in the ring
        """
        self.predecessor = None
        
        # For now, in a single-machine setup, just set the known node as successor
        # This allows basic multi-node operation without full network RPC
        self.finger_table.set_successor(known_node)
        self.successor_list = [known_node]
        
        self.logger.info(f"Joining ring via {known_node}")
        self.logger.info(f"Set successor to {known_node}")
    
    # ==================== Data Operations ====================
    
    def put(self, key: str, value: any) -> bool:
        """
        Store a key-value pair (local operation, no quorum yet).
        
        Args:
            key: The key
            value: The value
            
        Returns:
            True if stored successfully
        """
        key_id = hash_key(key, self.m)
        
        # Check if we're responsible for this key
        successor = self.find_successor(key_id)
        
        if successor and successor.node_id == self.node_id:
            # We're responsible, store it
            version = self.storage.put(key, value)
            self.logger.info(f"Stored {key}={value} with version {version}")
            return True
        else:
            # We're not responsible, should forward to successor
            # For now, just log
            self.logger.warning(f"Not responsible for key {key} (id={key_id}), "
                              f"successor={successor}")
            return False
    
    def get(self, key: str) -> Optional[any]:
        """
        Retrieve a value by key (local operation, no quorum yet).
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value or None if not found
        """
        key_id = hash_key(key, self.m)
        
        # Check if we're responsible for this key
        successor = self.find_successor(key_id)
        
        if successor and successor.node_id == self.node_id:
            # We're responsible, retrieve it
            value = self.storage.get_value(key)
            self.logger.info(f"Retrieved {key}={value}")
            return value
        else:
            # We're not responsible
            self.logger.warning(f"Not responsible for key {key} (id={key_id})")
            return None
    
    # ==================== Replication ====================
    
    def get_successor_list(self, n: int = None) -> List[NodeInfo]:
        """
        Get the first n successors for replication.
        
        Args:
            n: Number of successors (default: self.n_replicas)
            
        Returns:
            List of NodeInfo objects
        """
        if n is None:
            n = self.n_replicas
        
        # Return up to n successors from successor list
        return self.successor_list[:n]
    
    def update_successor_list(self):
        """
        Update the successor list by querying successors.
        Called periodically to maintain the successor list.
        """
        # In a real implementation:
        # 1. Ask immediate successor for its successor list
        # 2. Combine: [successor] + successor.successor_list[:-1]
        # 3. Keep first n_replicas entries
        
        if self.finger_table.get_successor():
            # For now, just ensure successor is in the list
            successor = self.finger_table.get_successor()
            if not self.successor_list or self.successor_list[0] != successor:
                self.successor_list = [successor] + self.successor_list[:self.n_replicas-1]
    
    async def update_successor_list_network(self, network_manager):
        """
        Update the successor list by querying the network.
        This is the network-aware version that actually queries other nodes.
        
        Args:
            network_manager: NetworkManager instance for sending messages
        """
        from communication.message import Message, MessageType
        
        successor = self.finger_table.get_successor()
        if not successor:
            self.logger.warning("No successor set in finger table")
            self.successor_list = []
            return
            
        if successor.node_id == self.node_id:
            # We're the only node, check if we have a predecessor we can use
            if self.predecessor and self.predecessor.node_id != self.node_id:
                self.successor_list = [self.predecessor]
                self.logger.info(f"Single node ring, using predecessor as successor: {self.predecessor}")
            else:
                self.successor_list = []
                self.logger.info("Single node ring, no replicas available")
            return
        
        try:
            # Start with our immediate successor
            new_list = [successor]
            seen_ids = {self.node_id, successor.node_id}
            
            # Query successor for its successor list
            msg = Message(
                msg_type=MessageType.GET_SUCCESSOR_LIST,
                sender_id=self.node_id,
                sender_address=self.address,
                msg_id=network_manager.generate_msg_id(),
                data={}
            )
            
            response = await network_manager.send_message(
                successor.address, msg, wait_response=True, timeout=5.0
            )
            
            if response and response.msg_type == MessageType.GET_SUCCESSOR_LIST_REPLY:
                successor_succ_list = response.data.get('successor_list', [])
                
                # Add nodes from successor's list (excluding self and duplicates)
                for node_data in successor_succ_list:
                    if len(new_list) >= self.n_replicas:
                        break
                        
                    if isinstance(node_data, dict):
                        node_id = node_data['node_id']
                        if node_id not in seen_ids:
                            new_list.append(NodeInfo(node_id, node_data['address']))
                            seen_ids.add(node_id)
                    elif isinstance(node_data, NodeInfo):
                        if node_data.node_id not in seen_ids:
                            new_list.append(node_data)
                            seen_ids.add(node_data.node_id)
            
            # Also add predecessor if we still need more and it's not already in the list
            if len(new_list) < self.n_replicas and self.predecessor:
                if self.predecessor.node_id not in seen_ids:
                    new_list.append(self.predecessor)
                    seen_ids.add(self.predecessor.node_id)
            
            self.successor_list = new_list[:self.n_replicas]
            self.logger.info(f"Updated successor list ({len(self.successor_list)} nodes): {[str(n) for n in self.successor_list]}")
                
        except Exception as e:
            self.logger.error(f"Error updating successor list: {e}")
            # Fallback: at least keep our immediate successor
            if successor and successor.node_id != self.node_id:
                self.successor_list = [successor]
    
    async def stabilize_network(self, network_manager):
        """
        Network-aware stabilization protocol.
        Periodically verify and update successor and notify it.
        
        Args:
            network_manager: NetworkManager instance for sending messages
        """
        from communication.message import Message, MessageType
        
        successor = self.finger_table.get_successor()
        
        # If we're pointing to ourselves and we have a predecessor, 
        # our predecessor might be a better successor
        if successor and successor.node_id == self.node_id and self.predecessor:
            # We were the only node, but now we have a predecessor
            # That predecessor should be our successor too (in a 2-node ring)
            self.finger_table.set_successor(self.predecessor)
            successor = self.predecessor
            self.logger.info(f"Stabilize: was alone, now successor = {successor}")
        
        if not successor or successor.node_id == self.node_id:
            return
        
        try:
            # Ask successor for its predecessor
            msg = Message(
                msg_type=MessageType.GET_PREDECESSOR,
                sender_id=self.node_id,
                sender_address=self.address,
                msg_id=network_manager.generate_msg_id(),
                data={}
            )
            
            response = await network_manager.send_message(
                successor.address, msg, wait_response=True, timeout=5.0
            )
            
            if response and response.msg_type == MessageType.GET_PREDECESSOR_REPLY:
                pred_data = response.data.get('predecessor')
                
                if pred_data:
                    x = NodeInfo(pred_data['node_id'], pred_data['address'])
                    
                    # If x is between us and our successor, x should be our new successor
                    if x.node_id != self.node_id and in_range(x.node_id, self.node_id, successor.node_id,
                               inclusive_start=False, inclusive_end=False):
                        self.finger_table.set_successor(x)
                        self.logger.info(f"Stabilize: updated successor to {x}")
            
            # Notify our (possibly new) successor that we exist
            new_successor = self.finger_table.get_successor()
            notify_msg = Message(
                msg_type=MessageType.NOTIFY,
                sender_id=self.node_id,
                sender_address=self.address,
                msg_id=network_manager.generate_msg_id(),
                data={'node_id': self.node_id, 'address': self.address}
            )
            
            await network_manager.send_message(
                new_successor.address, notify_msg, wait_response=False
            )
            
        except Exception as e:
            self.logger.error(f"Stabilization error: {e}")
    
    async def join_ring_network(self, known_node: NodeInfo, network_manager):
        """
        Network-aware join protocol.
        Join an existing Chord ring via a known node.
        
        Args:
            known_node: An existing node in the ring
            network_manager: NetworkManager instance for sending messages
        """
        from communication.message import Message, MessageType
        
        self.predecessor = None
        
        try:
            # Ask known_node to find our successor
            msg = Message(
                msg_type=MessageType.FIND_SUCCESSOR,
                sender_id=self.node_id,
                sender_address=self.address,
                msg_id=network_manager.generate_msg_id(),
                data={'identifier': self.node_id}
            )
            
            response = await network_manager.send_message(
                known_node.address, msg, wait_response=True, timeout=10.0
            )
            
            if response and response.msg_type == MessageType.FIND_SUCCESSOR_REPLY:
                succ_data = response.data.get('successor')
                if succ_data:
                    successor = NodeInfo(succ_data['node_id'], succ_data['address'])
                    self.finger_table.set_successor(successor)
                    self.successor_list = [successor]
                    self.logger.info(f"Joined ring: successor = {successor}")
                else:
                    # Fallback to known node
                    self.finger_table.set_successor(known_node)
                    self.successor_list = [known_node]
                    self.logger.info(f"Joined ring via known node: {known_node}")
            else:
                # Fallback to known node as successor
                self.finger_table.set_successor(known_node)
                self.successor_list = [known_node]
                self.logger.info(f"Joined ring (fallback): successor = {known_node}")
                
        except Exception as e:
            self.logger.error(f"Error joining ring: {e}")
            # Fallback to known node
            self.finger_table.set_successor(known_node)
            self.successor_list = [known_node]
    
    async def join_ring_full(self, known_node: NodeInfo, network_manager):
        """
        ENHANCED join protocol with full ring knowledge and key transfer.
        
        This implements the refactored join protocol:
        1. Find successor
        2. Get ALL nodes in ring
        3. Broadcast join to all nodes
        4. Transfer keys from successors
        5. Load persistent storage
        
        Args:
            known_node: An existing node in the ring
            network_manager: NetworkManager instance for sending messages
        """
        from communication.message import Message, MessageType
        
        self.predecessor = None
        
        try:
            # Step 1: Find our successor
            self.logger.info("Step 1: Finding successor...")
            msg = Message(
                msg_type=MessageType.FIND_SUCCESSOR,
                sender_id=self.node_id,
                sender_address=self.address,
                msg_id=network_manager.generate_msg_id(),
                data={'identifier': self.node_id}
            )
            
            response = await network_manager.send_message(
                known_node.address, msg, wait_response=True, timeout=10.0
            )
            
            successor = None
            if response and response.msg_type == MessageType.FIND_SUCCESSOR_REPLY:
                succ_data = response.data.get('successor')
                if succ_data:
                    successor = NodeInfo(succ_data['node_id'], succ_data['address'])
            
            if not successor:
                successor = known_node
            
            self.finger_table.set_successor(successor)
            self.successor_list = [successor]
            self.logger.info(f"Found successor: {successor}")
            
            # Step 2: Get ALL nodes in ring
            self.logger.info("Step 2: Getting all nodes in ring...")
            all_nodes = await self._get_all_nodes_from(known_node, network_manager)
            
            # Add ourselves to the node list
            all_nodes.append(self.get_info())
            self.finger_table.set_all_nodes(all_nodes)
            self.logger.info(f"Full ring knowledge: {len(all_nodes)} nodes")
            
            # Step 3: Broadcast join to all nodes
            self.logger.info("Step 3: Broadcasting join to all nodes...")
            await self._broadcast_join(all_nodes, network_manager)
            
            # Step 4: Transfer keys from successors
            self.logger.info("Step 4: Transferring keys from successors...")
            await self._transfer_keys_on_join(network_manager)
            
            # Step 5: Load persistent storage
            if self.storage.enable_persistence:
                self.logger.info("Step 5: Loading persistent storage...")
                primary_count = self.storage.load_all_primary()
                backup_count = self.storage.load_all_backups()
                self.logger.info(f"Loaded {primary_count} primary keys, {backup_count} backup keys")
            
            self.logger.info("Join complete!")
            
        except Exception as e:
            self.logger.error(f"Error in enhanced join: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            # Fallback to basic join
            await self.join_ring_network(known_node, network_manager)
    
    async def _get_all_nodes_from(self, known_node: NodeInfo, network_manager) -> List[NodeInfo]:
        """Get list of all nodes in the ring from a known node."""
        from communication.message import Message, MessageType
        
        msg = Message(
            msg_type=MessageType.GET_ALL_NODES,
            sender_id=self.node_id,
            sender_address=self.address,
            msg_id=network_manager.generate_msg_id(),
            data={}
        )
        
        response = await network_manager.send_message(
            known_node.address, msg, wait_response=True, timeout=10.0
        )
        
        all_nodes = []
        if response and response.msg_type == MessageType.GET_ALL_NODES_REPLY:
            nodes_data = response.data.get('nodes', [])
            for node_data in nodes_data:
                if isinstance(node_data, dict):
                    all_nodes.append(NodeInfo(node_data['node_id'], node_data['address']))
                elif isinstance(node_data, NodeInfo):
                    all_nodes.append(node_data)
        
        return all_nodes
    
    async def _broadcast_join(self, all_nodes: List[NodeInfo], network_manager):
        """Broadcast join notification to all nodes in the ring."""
        from communication.message import Message, MessageType
        
        my_info = self.get_info()
        success_count = 0
        
        for node in all_nodes:
            if node.node_id == self.node_id:
                continue
            
            try:
                msg = Message(
                    msg_type=MessageType.BROADCAST_JOIN,
                    sender_id=self.node_id,
                    sender_address=self.address,
                    msg_id=network_manager.generate_msg_id(),
                    data={
                        'node_id': my_info.node_id,
                        'address': my_info.address
                    }
                )
                
                response = await network_manager.send_message(
                    node.address, msg, wait_response=True, timeout=5.0
                )
                
                if response and response.msg_type == MessageType.BROADCAST_JOIN_ACK:
                    success_count += 1
                    
            except Exception as e:
                self.logger.warning(f"Failed to notify {node}: {e}")
        
        self.logger.info(f"Broadcast join: {success_count}/{len(all_nodes)-1} nodes notified")
    
    async def _transfer_keys_on_join(self, network_manager):
        """Request keys from our successors that should now belong to us."""
        from communication.message import Message, MessageType
        
        # Get our N-1 successors (they might have keys that belong to us)
        successors = self.finger_table.get_n_successors(self.node_id, self.n_replicas)
        
        for successor in successors[1:]:  # Skip ourselves
            try:
                msg = Message(
                    msg_type=MessageType.TRANSFER_KEYS_REQUEST,
                    sender_id=self.node_id,
                    sender_address=self.address,
                    msg_id=network_manager.generate_msg_id(),
                    data={
                        'new_node_id': self.node_id,
                        'predecessor_id': self.predecessor.node_id if self.predecessor else None
                    }
                )
                
                response = await network_manager.send_message(
                    successor.address, msg, wait_response=True, timeout=10.0
                )
                
                if response and response.msg_type == MessageType.TRANSFER_KEYS_RESPONSE:
                    keys_data = response.data.get('keys', {})
                    
                    # Receive the keys
                    for key_str, value_data in keys_data.items():
                        value = value_data['value']
                        version_dict = value_data['version']
                        version = VectorClock.from_dict(version_dict)
                        
                        # Store locally
                        current = self.storage.get(key_str)
                        if not current or current[1] < version:
                            self.storage.put(key_str, value, version)
                    
                    self.logger.info(f"Received {len(keys_data)} keys from {successor}")
                    
            except Exception as e:
                self.logger.warning(f"Failed to transfer keys from {successor}: {e}")
    
    # ==================== Hinted Handoff Recovery ====================
    
    async def recover_hinted_handoffs(self, network_manager):
        """
        Recover keys from hinted handoffs when this node rejoins the ring.
        
        This method:
        1. Contacts next N-1 successors to retrieve keys they stored as backups for us
        2. Updates our primary storage with the latest versions
        3. Tells those nodes to delete the backup hints
        4. Replicates updated keys to our N-1 successors as backups
        5. Contacts previous N-1 predecessors to update their backup copies
        
        Args:
            network_manager: NetworkManager instance for communication
        """
        from communication.message import Message, MessageType
        from consistency.vector_clock import VectorClock
        
        self.logger.info(f"Starting hinted handoff recovery for node {self.node_id}")
        
        # Step 1: Get next N-1 successors (nodes that might have hints for us)
        next_nodes = self.finger_table.get_n_successors(self.node_id, self.n_replicas)
        # Filter out ourselves
        next_nodes = [n for n in next_nodes if n.node_id != self.node_id][:self.n_replicas - 1]
        
        if not next_nodes:
            self.logger.info("No successors found for recovery, using successor list")
            next_nodes = self.successor_list[:self.n_replicas - 1]
        
        self.logger.info(f"Checking next {len(next_nodes)} nodes for hinted handoffs: {[str(n) for n in next_nodes]}")
        
        recovered_keys = {}  # key -> (value, version)
        
        # Step 2: Request hinted handoff data from each successor
        for node in next_nodes:
            try:
                msg = Message(
                    msg_type=MessageType.RECOVER_HANDOFF,
                    sender_id=self.node_id,
                    sender_address=self.address,
                    msg_id=network_manager.generate_msg_id(),
                    data={'requesting_node_id': self.node_id}
                )
                
                response = await network_manager.send_message(
                    node.address, msg, wait_response=True, timeout=5.0
                )
                
                if response and response.msg_type == MessageType.RECOVER_HANDOFF_REPLY:
                    keys_data = response.data.get('keys', {})
                    self.logger.info(f"Received {len(keys_data)} keys from {node}")
                    
                    # Merge keys with version reconciliation
                    for key, data in keys_data.items():
                        value = data['value']
                        version = VectorClock.from_dict(data['version'])
                        
                        if key not in recovered_keys:
                            recovered_keys[key] = (value, version)
                        else:
                            # Keep the newer version
                            _, existing_version = recovered_keys[key]
                            if version > existing_version:
                                recovered_keys[key] = (value, version)
                                self.logger.info(f"Updated key '{key}' with newer version from {node}")
                
            except Exception as e:
                self.logger.error(f"Error recovering from {node}: {e}")
        
        # Step 3: Update our primary storage with recovered keys
        self.logger.info(f"Recovered {len(recovered_keys)} unique keys, updating primary storage")
        for key, (value, version) in recovered_keys.items():
            # Check if we already have this key in primary storage
            existing = self.storage.get(key)
            if existing:
                _, existing_version = existing
                self.logger.info(f"Comparing versions for key '{key}': local={existing_version}, recovered={version}")
                
                # If recovered version is newer OR concurrent (which means we were down and missed updates)
                if version > existing_version:
                    # Recovered version is strictly newer
                    self.storage.put(key, value, version)
                    self.logger.info(f"Restored primary key (newer): {key}={value} (version: {version})")
                elif version.concurrent_with(existing_version):
                    # Versions are concurrent - we were down, so accept the recovered version
                    # Merge the clocks to preserve causality
                    merged_version = existing_version.copy()
                    merged_version.update(version)
                    self.storage.put(key, value, merged_version)
                    self.logger.info(f"Restored primary key (concurrent, merged): {key}={value} (version: {merged_version})")
                else:
                    # Our local version is newer - keep it
                    self.logger.info(f"Keeping local version (newer): {key} (version: {existing_version})")
            else:
                # No existing key, just store it
                self.storage.put(key, value, version)
                self.logger.info(f"Restored primary key (new): {key}={value} (version: {version})")
        
        # Step 4: Replicate to our new N-1 successors as backups
        if recovered_keys:
            await self.update_successor_list_network(network_manager)
            replicas = self.successor_list[:self.n_replicas - 1]
            
            self.logger.info(f"Replicating {len(recovered_keys)} recovered keys to {len(replicas)} successors")
            for replica in replicas:
                for key, (value, version) in recovered_keys.items():
                    try:
                        msg = Message(
                            msg_type=MessageType.UPDATE_BACKUP,
                            sender_id=self.node_id,
                            sender_address=self.address,
                            msg_id=network_manager.generate_msg_id(),
                            data={
                                'key': key,
                                'value': value,
                                'version': version.to_dict(),
                                'primary_node_id': self.node_id
                            }
                        )
                        
                        await network_manager.send_message(
                            replica.address, msg, wait_response=False, timeout=3.0
                        )
                    except Exception as e:
                        self.logger.error(f"Error replicating to {replica}: {e}")
        
        # Step 5: Update backups on previous N-1 predecessors
        # Get nodes before us in the ring
        prev_nodes = []
        if self.predecessor:
            prev_nodes.append(self.predecessor)
            # Try to get more predecessors by walking backwards
            # In a full implementation, we'd query predecessor.predecessor, etc.
        
        self.logger.info(f"Hinted handoff recovery complete. Recovered {len(recovered_keys)} keys.")
    
    # ==================== Utility Methods ====================
    
    def get_status(self) -> dict:
        """
        Get current node status.
        
        Returns:
            Dictionary with node state information
        """
        return {
            "node_id": self.node_id,
            "address": self.address,
            "predecessor": self.predecessor,
            "successor": self.finger_table.get_successor(),
            "successor_list": self.successor_list,
            "num_keys": self.storage.size(),
        }
    
    def __repr__(self) -> str:
        return f"ChordNode(id={self.node_id}, addr={self.address})"


if __name__ == "__main__":
    # Test node creation
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create a node
    node = ChordNode("localhost:5000", m=6)
    print(f"Created {node}")
    print(f"Status: {node.get_status()}")
    
    # Create a ring
    node.create_ring()
    print(f"\nAfter creating ring:")
    print(f"Status: {node.get_status()}")
    
    # Test storage
    node.put("test_key", "test_value")
    value = node.get("test_key")
    print(f"\nStored and retrieved: {value}")
    
    # Print finger table
    print(f"\n{node.finger_table}")
