"""
Vector Clock implementation for tracking causality and versioning.
"""

from typing import Dict, Optional
import copy


class VectorClock:
    """
    Vector clock for tracking causality between events in distributed systems.
    
    Each node maintains a vector of logical clocks, one per node in the system.
    When a node performs an operation, it increments its own clock.
    """
    
    def __init__(self, clock: Optional[Dict[int, int]] = None):
        """
        Initialize a vector clock.
        
        Args:
            clock: Optional dictionary mapping node_id -> timestamp
        """
        # Ensure all keys are integers (JSON serialization converts to strings)
        if clock is not None:
            self.clock: Dict[int, int] = {int(k): v for k, v in clock.items()}
        else:
            self.clock: Dict[int, int] = {}
    
    def increment(self, node_id: int) -> 'VectorClock':
        """
        Increment the clock for a specific node.
        
        Args:
            node_id: The node performing the operation
            
        Returns:
            Self for method chaining
        """
        self.clock[node_id] = self.clock.get(node_id, 0) + 1
        return self
    
    def update(self, other: 'VectorClock'):
        """
        Update this vector clock with another (take element-wise maximum).
        
        Args:
            other: Another vector clock to merge with
        """
        for node_id, timestamp in other.clock.items():
            self.clock[node_id] = max(self.clock.get(node_id, 0), timestamp)
    
    def merge(self, other: 'VectorClock', node_id: int) -> 'VectorClock':
        """
        Merge with another vector clock and increment for this node.
        Used when receiving a message: merge sender's clock and increment own.
        
        Args:
            other: The sender's vector clock
            node_id: This node's identifier
            
        Returns:
            Self for method chaining
        """
        self.update(other)
        self.increment(node_id)
        return self
    
    def copy(self) -> 'VectorClock':
        """
        Create a deep copy of this vector clock.
        
        Returns:
            New VectorClock instance with same values
        """
        return VectorClock(copy.deepcopy(self.clock))
    
    def happens_before(self, other: 'VectorClock') -> bool:
        """
        Check if this clock happens before another (strict partial order).
        
        self < other iff:
          - For all i: self[i] <= other[i]
          - For some i: self[i] < other[i]
        
        Args:
            other: Another vector clock
            
        Returns:
            True if this happens before other
        """
        all_less_or_equal = True
        at_least_one_less = False
        
        # Check all nodes in both clocks
        all_nodes = set(self.clock.keys()) | set(other.clock.keys())
        
        for node_id in all_nodes:
            self_val = self.clock.get(node_id, 0)
            other_val = other.clock.get(node_id, 0)
            
            if self_val > other_val:
                all_less_or_equal = False
                break
            elif self_val < other_val:
                at_least_one_less = True
        
        return all_less_or_equal and at_least_one_less
    
    def concurrent_with(self, other: 'VectorClock') -> bool:
        """
        Check if this clock is concurrent with another (incomparable).
        
        Two clocks are concurrent if neither happens before the other.
        
        Args:
            other: Another vector clock
            
        Returns:
            True if clocks are concurrent
        """
        return not self.happens_before(other) and not other.happens_before(self)
    
    def dominates(self, other: 'VectorClock') -> bool:
        """
        Check if this clock dominates (happens after or is equal to) another.
        
        self >= other iff: For all i: self[i] >= other[i]
        
        Args:
            other: Another vector clock
            
        Returns:
            True if this dominates other
        """
        all_nodes = set(self.clock.keys()) | set(other.clock.keys())
        
        for node_id in all_nodes:
            self_val = self.clock.get(node_id, 0)
            other_val = other.clock.get(node_id, 0)
            
            if self_val < other_val:
                return False
        
        return True
    
    def __eq__(self, other: object) -> bool:
        """Check if two vector clocks are equal."""
        if not isinstance(other, VectorClock):
            return False
        
        all_nodes = set(self.clock.keys()) | set(other.clock.keys())
        
        for node_id in all_nodes:
            if self.clock.get(node_id, 0) != other.clock.get(node_id, 0):
                return False
        
        return True
    
    def __lt__(self, other: 'VectorClock') -> bool:
        """Less than: happens before."""
        return self.happens_before(other)
    
    def __le__(self, other: 'VectorClock') -> bool:
        """Less than or equal: happens before or equal."""
        return self.happens_before(other) or self == other
    
    def __gt__(self, other: 'VectorClock') -> bool:
        """Greater than: other happens before this."""
        return other.happens_before(self)
    
    def __ge__(self, other: 'VectorClock') -> bool:
        """Greater than or equal."""
        return other.happens_before(self) or self == other
    
    def to_dict(self) -> Dict[str, int]:
        """
        Convert to dictionary representation suitable for JSON serialization.
        
        Returns:
            Dictionary mapping node_id (as string) -> timestamp
        """
        # Convert keys to strings for JSON compatibility
        return {str(k): v for k, v in self.clock.items()}
    
    @classmethod
    def from_dict(cls, data: Dict[int, int]) -> 'VectorClock':
        """
        Create a VectorClock from dictionary representation.
        
        Args:
            data: Dictionary mapping node_id -> timestamp
            
        Returns:
            New VectorClock instance
        """
        # Ensure all keys are integers (JSON serialization converts to strings)
        normalized = {int(k): v for k, v in data.items()}
        return cls(normalized)
    
    def __repr__(self) -> str:
        """String representation of the vector clock."""
        items = sorted(self.clock.items())
        clock_str = ", ".join(f"{node_id}:{ts}" for node_id, ts in items)
        return f"VectorClock({{{clock_str}}})"
    
    def __str__(self) -> str:
        """Human-readable string representation."""
        return self.__repr__()


def compare_versions(v1: VectorClock, v2: VectorClock) -> str:
    """
    Compare two vector clocks and return their relationship.
    
    Args:
        v1: First vector clock
        v2: Second vector clock
        
    Returns:
        String: "v1<v2", "v1>v2", "v1=v2", or "concurrent"
    """
    if v1 == v2:
        return "v1=v2"
    elif v1.happens_before(v2):
        return "v1<v2"
    elif v2.happens_before(v1):
        return "v1>v2"
    else:
        return "concurrent"


def get_latest_version(versions: list) -> Optional[VectorClock]:
    """
    Get the latest (most recent) version from a list of vector clocks.
    If there are concurrent versions, returns None (conflict).
    
    Args:
        versions: List of VectorClock instances
        
    Returns:
        The latest VectorClock, or None if there's a conflict
    """
    if not versions:
        return None
    
    if len(versions) == 1:
        return versions[0]
    
    # Find the maximum (if it exists)
    candidates = []
    
    for v in versions:
        is_dominated = False
        for other in versions:
            if other.happens_before(v):
                continue
            elif v.happens_before(other):
                is_dominated = True
                break
        
        if not is_dominated:
            candidates.append(v)
    
    # If exactly one candidate, it's the latest
    if len(candidates) == 1:
        return candidates[0]
    
    # Multiple candidates means concurrent updates (conflict)
    return None


if __name__ == "__main__":
    # Example usage and tests
    print("Vector Clock Example:\n")
    
    # Node 1 performs operation
    v1 = VectorClock()
    v1.increment(1)
    print(f"Node 1 increments: {v1}")
    
    # Node 2 performs operation
    v2 = VectorClock()
    v2.increment(2)
    print(f"Node 2 increments: {v2}")
    
    # Node 1 receives message from Node 2
    v1_new = v1.copy()
    v1_new.merge(v2, 1)
    print(f"Node 1 merges with v2: {v1_new}")
    
    # Check relationships
    print(f"\nv1 < v1_new: {v1.happens_before(v1_new)}")
    print(f"v1 concurrent with v2: {v1.concurrent_with(v2)}")
    print(f"Comparison: {compare_versions(v1, v2)}")
