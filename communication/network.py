"""
Network manager for socket-based communication between Chord nodes.
"""

import asyncio
import logging
from typing import Optional, Callable, Dict
from .message import Message, MessageType
import uuid


class NetworkManager:
    """
    Manages network communication for a Chord node using asyncio.
    
    Handles:
    - Listening for incoming connections
    - Sending messages to other nodes
    - Message routing and callbacks
    """
    
    def __init__(self, node_id: int, host: str, port: int):
        """
        Initialize network manager.
        
        Args:
            node_id: This node's identifier
            host: Host to bind to
            port: Port to listen on
        """
        self.node_id = node_id
        self.host = host
        self.port = port
        self.address = f"{host}:{port}"
        
        # Server
        self.server: Optional[asyncio.Server] = None
        
        # Message handlers: msg_type -> callback function
        self.handlers: Dict[MessageType, Callable] = {}
        
        # Pending requests: msg_id -> Future
        self.pending: Dict[str, asyncio.Future] = {}
        
        self.logger = logging.getLogger(f"Network-{node_id}")
    
    def register_handler(self, msg_type: MessageType, handler: Callable):
        """
        Register a message handler.
        
        Args:
            msg_type: Type of message to handle
            handler: Async callback function
        """
        self.handlers[msg_type] = handler
        self.logger.debug(f"Registered handler for {msg_type.value}")
    
    async def start(self):
        """Start the network server."""
        self.server = await asyncio.start_server(
            self._handle_connection,
            self.host,
            self.port
        )
        
        self.logger.info(f"Network server started on {self.address}")
    
    async def stop(self):
        """Stop the network server."""
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.logger.info("Network server stopped")
    
    async def _handle_connection(self, reader: asyncio.StreamReader, 
                                 writer: asyncio.StreamWriter):
        """
        Handle an incoming connection.
        
        Args:
            reader: Stream reader
            writer: Stream writer
        """
        addr = writer.get_extra_info('peername')
        self.logger.debug(f"Connection from {addr}")
        
        try:
            # Read message length (4 bytes)
            length_bytes = await reader.readexactly(4)
            msg_length = int.from_bytes(length_bytes, byteorder='big')
            
            # Read message data
            msg_data = await reader.readexactly(msg_length)
            
            # Deserialize message
            msg = Message.from_bytes(msg_data)
            self.logger.debug(f"Received: {msg}")
            
            # Handle message
            response = await self._dispatch_message(msg)
            
            # Send response if any
            if response:
                await self._send_message(writer, response)
            
        except asyncio.IncompleteReadError:
            self.logger.debug(f"Connection closed by {addr}")
        except ConnectionResetError:
            self.logger.debug(f"Connection reset by {addr}")
        except BrokenPipeError:
            self.logger.debug(f"Broken pipe for {addr}")
        except Exception as e:
            # Only log unexpected errors
            error_msg = str(e).lower()
            if 'connection' not in error_msg and 'closed' not in error_msg:
                self.logger.error(f"Error handling connection from {addr}: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except (BrokenPipeError, ConnectionResetError, OSError):
                # Connection already closed by peer, ignore
                pass
    
    async def _dispatch_message(self, msg: Message) -> Optional[Message]:
        """
        Dispatch message to appropriate handler.
        
        Args:
            msg: Received message
            
        Returns:
            Response message or None
        """
        self.logger.debug(f"Dispatching message: {msg.msg_type} from {msg.sender_id}")
        
        # Check if this is a reply to a pending request
        if msg.msg_id in self.pending:
            future = self.pending.pop(msg.msg_id)
            future.set_result(msg)
            return None
        
        # Otherwise, dispatch to handler
        handler = self.handlers.get(msg.msg_type)
        
        if handler:
            try:
                self.logger.debug(f"Calling handler for {msg.msg_type}")
                response = await handler(msg)
                self.logger.debug(f"Handler returned: {response}")
                return response
            except Exception as e:
                self.logger.error(f"Handler error for {msg.msg_type}: {e}")
                import traceback
                traceback.print_exc()
                # Return error message
                from .message import create_error_msg
                return create_error_msg(
                    self.node_id,
                    self.address,
                    str(e),
                    msg.msg_id
                )
        else:
            self.logger.warning(f"No handler for {msg.msg_type}")
            return None
    
    async def send_message(self, target_address: str, msg: Message,
                          wait_response: bool = False,
                          timeout: float = 5.0) -> Optional[Message]:
        """
        Send a message to another node.
        
        Args:
            target_address: Target node address "host:port"
            msg: Message to send
            wait_response: Whether to wait for a response
            timeout: Timeout in seconds
            
        Returns:
            Response message if wait_response=True, else None
        """
        writer = None
        try:
            # Parse target address
            host, port = target_address.split(':')
            port = int(port)
            
            # Connect to target
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout
            )
            
            # Send message
            await self._send_message(writer, msg)
            
            # Wait for response if requested
            if wait_response:
                try:
                    # Read response length (4 bytes)
                    length_bytes = await asyncio.wait_for(
                        reader.readexactly(4),
                        timeout=timeout
                    )
                    msg_length = int.from_bytes(length_bytes, byteorder='big')
                    
                    # Read response data
                    msg_data = await asyncio.wait_for(
                        reader.readexactly(msg_length),
                        timeout=timeout
                    )
                    
                    # Deserialize response
                    response = Message.from_bytes(msg_data)
                    self.logger.debug(f"Received response: {response}")
                    
                    return response
                    
                except asyncio.TimeoutError:
                    self.logger.warning(f"Response timeout for {msg.msg_id}")
                    return None
                except Exception as e:
                    self.logger.error(f"Error reading response: {e}")
                    return None
            else:
                return None
            
        except asyncio.TimeoutError:
            self.logger.error(f"Connection timeout to {target_address}")
            return None
        except ConnectionRefusedError:
            self.logger.error(f"Connection refused by {target_address}")
            return None
        except Exception as e:
            self.logger.error(f"Error sending to {target_address}: {type(e).__name__}: {e}")
            return None
        finally:
            if writer:
                try:
                    writer.close()
                    await writer.wait_closed()
                except (BrokenPipeError, ConnectionResetError, OSError):
                    # Connection already closed, ignore
                    pass
    
    async def _send_message(self, writer: asyncio.StreamWriter, msg: Message):
        """
        Send a message through a writer stream.
        
        Args:
            writer: Stream writer
            msg: Message to send
        """
        msg_bytes = msg.to_bytes()
        msg_length = len(msg_bytes)
        
        # Send length prefix (4 bytes)
        writer.write(msg_length.to_bytes(4, byteorder='big'))
        
        # Send message data
        writer.write(msg_bytes)
        
        await writer.drain()
        self.logger.debug(f"Sent: {msg}")
    
    def generate_msg_id(self) -> str:
        """Generate a unique message ID."""
        return str(uuid.uuid4())
    
    def __repr__(self) -> str:
        return f"NetworkManager(node={self.node_id}, addr={self.address})"


if __name__ == "__main__":
    # Test network manager
    import sys
    from .message import create_put_msg
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    async def test():
        # Create two network managers
        nm1 = NetworkManager(node_id=1, host='localhost', port=5000)
        nm2 = NetworkManager(node_id=2, host='localhost', port=5001)
        
        # Define a simple handler for node 2
        async def handle_put(msg: Message) -> Message:
            print(f"Node 2 received PUT: {msg.data}")
            from .message import create_reply_msg, MessageType
            return create_reply_msg(
                nm2.node_id,
                nm2.address,
                MessageType.PUT_REPLY,
                {'status': 'ok'},
                msg.msg_id
            )
        
        # Register handler
        nm2.register_handler(MessageType.PUT, handle_put)
        
        # Start servers
        await nm1.start()
        await nm2.start()
        
        print("Servers started\n")
        
        # Send a message from node 1 to node 2
        msg = create_put_msg(
            nm1.node_id,
            nm1.address,
            'test_key',
            'test_value',
            nm1.generate_msg_id()
        )
        
        print(f"Sending from Node 1 to Node 2...")
        response = await nm1.send_message(nm2.address, msg, wait_response=True)
        
        if response:
            print(f"Response: {response.data}")
        
        # Cleanup
        await nm1.stop()
        await nm2.stop()
    
    asyncio.run(test())
