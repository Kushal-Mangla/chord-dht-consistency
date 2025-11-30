"""
Finger table and routing logic for Chord DHT.
"""

import hashlib
from typing import Optional, Tuple, List
from dataclasses import dataclass


@dataclass
class NodeInfo:
    """Information about a Chord node."""
    node_id: int
    address: str  # "host:port"
    
    def __repr__(self) -> str:
        return f"Node({self.node_id}, {self.address})"
    
    def __eq__(self, other) -> bool:
        if not isinstance(other, NodeInfo):
            return False
        return self.node_id == other.node_id
    
    def __hash__(self) -> int:
        return hash(self.node_id)


class FingerTable:
    """
    Finger table for efficient routing in Chord.
    
    ENHANCED: Now stores ALL nodes in the ring for full ring knowledge.
    This enables efficient quorum operations and better fault tolerance.
    
    The i-th entry in the finger table of node n contains the first node
    that succeeds n by at least 2^(i-1) on the identifier circle.
    """
    
    def __init__(self, node_id: int, m: int):
        """
        Initialize finger table for a node.
        
        Args:
            node_id: This node's identifier
            m: Bit size of identifier space (supports 2^m nodes)
        """
        self.node_id = node_id
        self.m = m
        self.max_id = 2 ** m
        
        # Finger table: list of m entries (for backwards compatibility)
        # Each entry is (start, interval, node)
        # where start = (n + 2^(i-1)) mod 2^m
        self.fingers: List[Optional[NodeInfo]] = [None] * m
        
        # Cache the start values for each finger
        self.starts = [(node_id + 2**i) % self.max_id for i in range(m)]
        
        # NEW: Full ring knowledge - ALL nodes sorted by ID
        self.all_nodes: List[NodeInfo] = []
    
    def get_finger(self, index: int) -> Optional[NodeInfo]:
        """
        Get the node at finger table index.
        
        Args:
            index: Finger table index (0 to m-1)
            
        Returns:
            NodeInfo or None if not set
        """
        if 0 <= index < self.m:
            return self.fingers[index]
        return None
    
    def set_finger(self, index: int, node: NodeInfo):
        """
        Set a finger table entry.
        
        Args:
            index: Finger table index (0 to m-1)
            node: NodeInfo to set
        """
        if 0 <= index < self.m:
            self.fingers[index] = node
    
    def get_start(self, index: int) -> int:
        """
        Get the start of the interval for finger i.
        
        Args:
            index: Finger table index (0 to m-1)
            
        Returns:
            Start identifier
        """
        if 0 <= index < self.m:
            return self.starts[index]
        return -1
    
    def find_closest_preceding_finger(self, identifier: int) -> Optional[NodeInfo]:
        """
        Find the closest finger preceding the given identifier.
        
        Args:
            identifier: The identifier to search for
            
        Returns:
            NodeInfo of closest preceding node, or None
        """
        # Search from highest finger to lowest
        for i in range(self.m - 1, -1, -1):
            finger = self.fingers[i]
            if finger and in_range(finger.node_id, self.node_id, identifier, 
                                   inclusive_start=False):
                return finger
        return None
    
    def get_successor(self) -> Optional[NodeInfo]:
        """
        Get the immediate successor (first finger entry).
        
        Returns:
            NodeInfo of successor or None
        """
        return self.fingers[0] if self.fingers else None
    
    def set_successor(self, node: NodeInfo):
        """
        Set the immediate successor (first finger entry).
        
        Args:
            node: NodeInfo of the successor
        """
        self.fingers[0] = node
    
    def get_all_fingers(self) -> List[Optional[NodeInfo]]:
        """
        Get all finger table entries.
        
        Returns:
            List of NodeInfo objects (may contain None)
        """
        return self.fingers.copy()
    
    def clear(self):
        """Clear all finger table entries."""
        self.fingers = [None] * self.m
        self.all_nodes = []
    
    # ==================== NEW: Full Ring Knowledge Methods ====================
    
    def set_all_nodes(self, nodes: List[NodeInfo]):
        """
        Replace entire node list with full ring knowledge.
        
        Args:
            nodes: List of all nodes in the ring
        """
        # Store unique nodes sorted by ID
        unique_nodes = {node.node_id: node for node in nodes if node}
        self.all_nodes = sorted(unique_nodes.values(), key=lambda n: n.node_id)
        
        # Also update first finger entry (successor) if we have nodes
        if self.all_nodes:
            # Find our successor (first node with ID >= ours, or wrap to first)
            for node in self.all_nodes:
                if node.node_id > self.node_id:
                    self.set_successor(node)
                    break
            else:
                # Wrap around to first node
                self.set_successor(self.all_nodes[0])
    
    def add_node(self, node: NodeInfo):
        """
        Add a node to the full ring knowledge.
        
        Args:
            node: NodeInfo to add
        """
        # Check if node already exists
        for existing in self.all_nodes:
            if existing.node_id == node.node_id:
                # Update address if different
                if existing.address != node.address:
                    existing.address = node.address
                return
        
        # Add and re-sort
        self.all_nodes.append(node)
        self.all_nodes.sort(key=lambda n: n.node_id)
        
        # Update successor if needed
        successor = self.get_successor()
        if not successor or (node.node_id > self.node_id and 
                            (node.node_id < successor.node_id or successor.node_id <= self.node_id)):
            self.set_successor(node)
    
    def remove_node(self, node_id: int) -> bool:
        """
        Remove a node from the full ring knowledge.
        
        Args:
            node_id: ID of the node to remove
            
        Returns:
            True if removed, False if not found
        """
        self.all_nodes = [n for n in self.all_nodes if n.node_id != node_id]
        
        # Update successor if we removed it
        successor = self.get_successor()
        if successor and successor.node_id == node_id:
            if self.all_nodes:
                # Find new successor
                for node in self.all_nodes:
                    if node.node_id > self.node_id:
                        self.set_successor(node)
                        break
                else:
                    self.set_successor(self.all_nodes[0])
            else:
                self.set_successor(None)
        
        # Also remove from finger table
        for i in range(self.m):
            if self.fingers[i] and self.fingers[i].node_id == node_id:
                self.fingers[i] = None
        
        return True
    
    def get_all_nodes(self) -> List[NodeInfo]:
        """
        Get all known nodes in the ring.
        
        Returns:
            List of all NodeInfo objects sorted by ID
        """
        return self.all_nodes.copy()
    
    def find_successor_from_all(self, identifier: int) -> Optional[NodeInfo]:
        """
        Find the responsible node for an identifier using full ring knowledge.
        
        Args:
            identifier: The identifier to look up
            
        Returns:
            NodeInfo of the responsible node, or None if no nodes
        """
        if not self.all_nodes:
            return None
        
        # Find first node with ID >= identifier
        for node in self.all_nodes:
            if node.node_id >= identifier:
                return node
        
        # Wrap around to first node
        return self.all_nodes[0]
    
    def get_n_successors(self, identifier: int, n: int) -> List[NodeInfo]:
        """
        Get N successive nodes starting from the responsible node for an identifier.
        Used for replication.
        
        Args:
            identifier: The identifier to look up
            n: Number of successors to return
            
        Returns:
            List of up to N NodeInfo objects
        """
        if not self.all_nodes or n <= 0:
            return []
        
        # Find the responsible node (first successor)
        responsible = self.find_successor_from_all(identifier)
        if not responsible:
            return []
        
        # Find index of responsible node
        try:
            start_idx = self.all_nodes.index(responsible)
        except ValueError:
            return [responsible]
        
        # Get n nodes starting from responsible, wrapping around
        result = []
        num_nodes = len(self.all_nodes)
        for i in range(min(n, num_nodes)):
            idx = (start_idx + i) % num_nodes
            result.append(self.all_nodes[idx])
        
        return result
    
    # ==================== End of Full Ring Knowledge Methods ====================
    
    def __repr__(self) -> str:
        entries = []
        for i in range(self.m):
            start = self.starts[i]
            finger = self.fingers[i]
            if finger:
                entries.append(f"  [{i}] start={start} -> {finger}")
        
        entries_str = "\n".join(entries) if entries else "  (empty)"
        return f"FingerTable for Node {self.node_id}:\n{entries_str}"


def hash_address(address: str, m: int = None) -> int:
    """
    Hash a node address to an identifier.
    
    Args:
        address: Node address (e.g., "192.168.1.1:5000")
        m: Bit size of identifier space
        
    Returns:
        Integer identifier in range [0, 2^m)
    """
    if m is None:
        from config import M
        m = M
    
    hash_obj = hashlib.sha1(address.encode())
    hash_bytes = hash_obj.digest()
    hash_int = int.from_bytes(hash_bytes, byteorder='big')
    return hash_int % (2 ** m)


def in_range(identifier: int, start: int, end: int, 
             inclusive_start: bool = False, inclusive_end: bool = True) -> bool:
    """
    Check if identifier is in range on the circular identifier space.
    
    Args:
        identifier: The identifier to check
        start: Start of range
        end: End of range
        inclusive_start: Include start in range
        inclusive_end: Include end in range
        
    Returns:
        True if identifier is in range
    """
    if start == end:
        # Full circle
        return True
    
    # Adjust for inclusivity
    if start < end:
        # Normal range
        if inclusive_start and inclusive_end:
            return start <= identifier <= end
        elif inclusive_start and not inclusive_end:
            return start <= identifier < end
        elif not inclusive_start and inclusive_end:
            return start < identifier <= end
        else:  # not inclusive_start and not inclusive_end
            return start < identifier < end
    else:
        # Wraparound range
        if inclusive_start and inclusive_end:
            return identifier >= start or identifier <= end
        elif inclusive_start and not inclusive_end:
            return identifier >= start or identifier < end
        elif not inclusive_start and inclusive_end:
            return identifier > start or identifier <= end
        else:  # not inclusive_start and not inclusive_end
            return identifier > start or identifier < end


if __name__ == "__main__":
    # Test finger table
    from config import M
    
    print(f"Testing FingerTable with M={M}\n")
    
    # Create a finger table for node 10
    ft = FingerTable(node_id=10, m=M)
    
    print("Finger table starts:")
    for i in range(M):
        print(f"  Finger {i}: start = {ft.get_start(i)}")
    
    # Set some fingers
    ft.set_finger(0, NodeInfo(15, "localhost:5001"))
    ft.set_finger(1, NodeInfo(20, "localhost:5002"))
    ft.set_finger(2, NodeInfo(30, "localhost:5003"))
    
    print(f"\n{ft}")
    
    # Test range checking
    print("\nRange tests:")
    print(f"Is 12 in (10, 15]? {in_range(12, 10, 15)}")
    print(f"Is 5 in (60, 10]? {in_range(5, 60, 10)}")  # Wraparound
    print(f"Is 62 in (60, 10]? {in_range(62, 60, 10)}")  # Wraparound
