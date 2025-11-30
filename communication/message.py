"""
Message protocol definitions for Chord communication.
"""

from enum import Enum
from typing import Any, Dict, Optional
from dataclasses import dataclass, asdict
import json


class MessageType(Enum):
    """Types of messages exchanged between Chord nodes."""
    
    # Chord protocol messages
    FIND_SUCCESSOR = "find_successor"
    FIND_SUCCESSOR_REPLY = "find_successor_reply"
    GET_PREDECESSOR = "get_predecessor"
    GET_PREDECESSOR_REPLY = "get_predecessor_reply"
    GET_SUCCESSOR_LIST = "get_successor_list"
    GET_SUCCESSOR_LIST_REPLY = "get_successor_list_reply"
    NOTIFY = "notify"
    NOTIFY_ACK = "notify_ack"
    STABILIZE = "stabilize"
    
    # Data operations
    PUT = "put"
    PUT_REPLY = "put_reply"
    GET = "get"
    GET_REPLY = "get_reply"
    DELETE = "delete"
    DELETE_REPLY = "delete_reply"
    
    # Replication messages
    PUT_REPLICA = "put_replica"
    PUT_REPLICA_ACK = "put_replica_ack"
    PUT_REPLICA_REPLY = "put_replica_reply"  # Alias for ACK
    GET_REPLICA = "get_replica"
    GET_REPLICA_REPLY = "get_replica_reply"
    
    # Node management
    JOIN = "join"
    LEAVE = "leave"
    PING = "ping"
    PONG = "pong"
    
    # NEW: Enhanced join and key transfer messages
    BROADCAST_JOIN = "broadcast_join"
    BROADCAST_JOIN_ACK = "broadcast_join_ack"
    TRANSFER_KEYS_REQUEST = "transfer_keys_request"
    TRANSFER_KEYS_RESPONSE = "transfer_keys_response"
    GET_ALL_NODES = "get_all_nodes"
    GET_ALL_NODES_REPLY = "get_all_nodes_reply"
    
    # NEW: Hinted handoff recovery messages
    RECOVER_HANDOFF = "recover_handoff"
    RECOVER_HANDOFF_REPLY = "recover_handoff_reply"
    UPDATE_BACKUP = "update_backup"
    UPDATE_BACKUP_ACK = "update_backup_ack"
    
    # Debug/Status
    GET_ALL_KEYS = "get_all_keys"
    GET_ALL_KEYS_REPLY = "get_all_keys_reply"
    GET_RING_INFO = "get_ring_info"
    GET_RING_INFO_REPLY = "get_ring_info_reply"
    
    # Error
    ERROR = "error"


@dataclass
class Message:
    """
    Base message class for Chord communication.
    
    All messages between nodes follow this structure.
    """
    
    msg_type: MessageType
    sender_id: int
    sender_address: str
    msg_id: str  # Unique message identifier
    data: Dict[str, Any]
    
    def to_json(self) -> str:
        """
        Serialize message to JSON string.
        
        Returns:
            JSON string representation
        """
        msg_dict = {
            'msg_type': self.msg_type.value,
            'sender_id': self.sender_id,
            'sender_address': self.sender_address,
            'msg_id': self.msg_id,
            'data': self.data
        }
        return json.dumps(msg_dict)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Message':
        """
        Deserialize message from JSON string.
        
        Args:
            json_str: JSON string
            
        Returns:
            Message instance
        """
        msg_dict = json.loads(json_str)
        msg_dict['msg_type'] = MessageType(msg_dict['msg_type'])
        return cls(**msg_dict)
    
    def to_bytes(self) -> bytes:
        """Convert message to bytes for network transmission."""
        return self.to_json().encode('utf-8')
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'Message':
        """
        Create message from bytes.
        
        Args:
            data: Byte data
            
        Returns:
            Message instance
        """
        return cls.from_json(data.decode('utf-8'))
    
    def __repr__(self) -> str:
        return (f"Message({self.msg_type.value}, "
                f"from={self.sender_id}, id={self.msg_id})")


# Helper functions to create common messages

def create_find_successor_msg(sender_id: int, sender_addr: str, 
                               identifier: int, msg_id: str) -> Message:
    """Create FIND_SUCCESSOR message."""
    return Message(
        msg_type=MessageType.FIND_SUCCESSOR,
        sender_id=sender_id,
        sender_address=sender_addr,
        msg_id=msg_id,
        data={'identifier': identifier}
    )


def create_put_msg(sender_id: int, sender_addr: str,
                   key: str, value: Any, msg_id: str) -> Message:
    """Create PUT message."""
    return Message(
        msg_type=MessageType.PUT,
        sender_id=sender_id,
        sender_address=sender_addr,
        msg_id=msg_id,
        data={'key': key, 'value': value}
    )


def create_get_msg(sender_id: int, sender_addr: str,
                   key: str, msg_id: str) -> Message:
    """Create GET message."""
    return Message(
        msg_type=MessageType.GET,
        sender_id=sender_id,
        sender_address=sender_addr,
        msg_id=msg_id,
        data={'key': key}
    )


def create_put_replica_msg(sender_id: int, sender_addr: str,
                           key: str, value: Any, version: Dict,
                           msg_id: str) -> Message:
    """Create PUT_REPLICA message."""
    return Message(
        msg_type=MessageType.PUT_REPLICA,
        sender_id=sender_id,
        sender_address=sender_addr,
        msg_id=msg_id,
        data={'key': key, 'value': value, 'version': version}
    )


def create_reply_msg(sender_id: int, sender_addr: str,
                    msg_type: MessageType, data: Dict,
                    msg_id: str) -> Message:
    """Create a reply message."""
    return Message(
        msg_type=msg_type,
        sender_id=sender_id,
        sender_address=sender_addr,
        msg_id=msg_id,
        data=data
    )


def create_error_msg(sender_id: int, sender_addr: str,
                    error: str, msg_id: str) -> Message:
    """Create ERROR message."""
    return Message(
        msg_type=MessageType.ERROR,
        sender_id=sender_id,
        sender_address=sender_addr,
        msg_id=msg_id,
        data={'error': error}
    )


if __name__ == "__main__":
    # Test message serialization
    import uuid
    
    print("Testing Message Protocol:\n")
    
    # Create a message
    msg = create_put_msg(
        sender_id=1,
        sender_addr="localhost:5000",
        key="test_key",
        value="test_value",
        msg_id=str(uuid.uuid4())
    )
    
    print(f"Original: {msg}")
    print(f"Data: {msg.data}")
    
    # Serialize to JSON
    json_str = msg.to_json()
    print(f"\nJSON: {json_str}")
    
    # Deserialize
    msg2 = Message.from_json(json_str)
    print(f"\nDeserialized: {msg2}")
    print(f"Data: {msg2.data}")
    
    # Test bytes conversion
    msg_bytes = msg.to_bytes()
    print(f"\nBytes: {msg_bytes[:100]}...")
    
    msg3 = Message.from_bytes(msg_bytes)
    print(f"From bytes: {msg3}")
