"""
Command-line client for interacting with Chord DHT.
"""

import asyncio
import sys
from typing import Optional
from communication.message import create_put_msg, create_get_msg, Message, MessageType
from communication.network import NetworkManager
from chord.storage import hash_key
import config


class ChordClient:
    """
    Client for issuing commands to a Chord network.
    """
    
    def __init__(self, node_address: str):
        """
        Initialize client.
        
        Args:
            node_address: Address of a Chord node to connect to
        """
        self.node_address = node_address
        self.client_id = 0  # Client has ID 0
        self.network = NetworkManager(
            node_id=self.client_id,
            host='localhost',
            port=0  # Use any available port
        )
        self.ring_info = None  # Cached ring info
    
    async def connect(self):
        """Connect to the Chord network."""
        await self.network.start()
        print(f"Connected to Chord network via {self.node_address}")
        # Fetch ring info on connect
        await self.refresh_ring_info()
    
    async def refresh_ring_info(self):
        """Fetch ring topology from the network."""
        msg = Message(
            msg_type=MessageType.GET_RING_INFO,
            sender_id=self.client_id,
            sender_address="client",
            msg_id=self.network.generate_msg_id(),
            data={}
        )
        
        response = await self.network.send_message(
            self.node_address,
            msg,
            wait_response=True,
            timeout=5.0
        )
        
        if response and response.msg_type == MessageType.GET_RING_INFO_REPLY:
            self.ring_info = response.data
            return True
        return False
    
    async def discover_all_nodes(self) -> list:
        """
        Discover all nodes in the ring by following successors.
        
        Returns:
            List of all discovered nodes with their info
        """
        all_nodes = {}  # {address: node_info} - nodes we know about
        visited = set()  # addresses we've actually queried
        to_visit = [self.node_address]
        
        while to_visit:
            addr = to_visit.pop(0)
            if addr in visited:
                continue
            visited.add(addr)
            
            # Query this node for ring info
            msg = Message(
                msg_type=MessageType.GET_RING_INFO,
                sender_id=self.client_id,
                sender_address="client",
                msg_id=self.network.generate_msg_id(),
                data={}
            )
            
            try:
                response = await self.network.send_message(
                    addr, msg, wait_response=True, timeout=2.0
                )
                
                if response and response.msg_type == MessageType.GET_RING_INFO_REPLY:
                    ring_nodes = response.data.get('ring_nodes', [])
                    for n in ring_nodes:
                        node_addr = n['address']
                        # Always update node info (might have more details now)
                        all_nodes[node_addr] = n
                        # Add to visit queue if not yet visited
                        if node_addr not in visited and node_addr not in to_visit:
                            to_visit.append(node_addr)
                        
                        # Also follow successor/predecessor links
                        succ = n.get('successor')
                        if succ and succ.get('address'):
                            succ_addr = succ['address']
                            if succ_addr not in visited and succ_addr not in to_visit:
                                to_visit.append(succ_addr)
                            if succ_addr not in all_nodes:
                                all_nodes[succ_addr] = {
                                    'node_id': succ.get('node_id'),
                                    'address': succ_addr
                                }
                        
                        pred = n.get('predecessor')
                        if pred and pred.get('address'):
                            pred_addr = pred['address']
                            if pred_addr not in visited and pred_addr not in to_visit:
                                to_visit.append(pred_addr)
                            if pred_addr not in all_nodes:
                                all_nodes[pred_addr] = {
                                    'node_id': pred.get('node_id'),
                                    'address': pred_addr
                                }
            except Exception as e:
                print(f"Warning: Could not query {addr}: {e}")
        
        return list(all_nodes.values())
    
    def get_responsible_node(self, key_hash: int) -> dict:
        """
        Determine which node is responsible for a key hash based on ring info.
        
        Args:
            key_hash: Hash of the key
            
        Returns:
            Dict with node_id and address, or None
        """
        if not self.ring_info or 'ring_nodes' not in self.ring_info:
            return None
        
        nodes = self.ring_info['ring_nodes']
        if not nodes:
            return None
        
        # Sort nodes by node_id
        sorted_nodes = sorted(nodes, key=lambda x: x['node_id'])
        
        # Find the successor (first node with id >= key_hash)
        for node in sorted_nodes:
            if node['node_id'] >= key_hash:
                return node
        
        # Wrap around - first node in the ring
        return sorted_nodes[0]
    
    async def disconnect(self):
        """Disconnect from the Chord network."""
        await self.network.stop()
        print("Disconnected")
    
    async def put(self, key: str, value: str) -> bool:
        """
        Store a key-value pair.
        
        Args:
            key: Key to store
            value: Value to store
            
        Returns:
            True if successful
        """
        key_hash = hash_key(key, config.M)
        print(f"Key '{key}' -> hash={key_hash} (in ring space 0-{2**config.M - 1})")
        
        msg = create_put_msg(
            self.client_id,
            "client",
            key,
            value,
            self.network.generate_msg_id()
        )
        
        response = await self.network.send_message(
            self.node_address,
            msg,
            wait_response=True,
            timeout=5.0
        )
        
        if response:
            if response.msg_type == MessageType.ERROR:
                error = response.data.get('error', 'Unknown error')
                print(f"✗ PUT failed: {error}")
                return False
            elif response.msg_type == MessageType.PUT_REPLY:
                status = response.data.get('status')
                if status == 'ok':
                    print(f"✓ PUT {key} = {value}")
                    return True
                else:
                    error = response.data.get('error', 'Unknown error')
                    print(f"✗ PUT failed: {error}")
                    return False
            else:
                print(f"✗ PUT failed: Unexpected response type {response.msg_type}")
                return False
        else:
            print(f"✗ PUT failed: No response")
            return False
    
    async def get(self, key: str) -> Optional[str]:
        """
        Retrieve a value by key.
        
        Args:
            key: Key to retrieve
            
        Returns:
            Value or None if not found
        """
        key_hash = hash_key(key, config.M)
        print(f"Key '{key}' -> hash={key_hash} (in ring space 0-{2**config.M - 1})")
        
        msg = create_get_msg(
            self.client_id,
            "client",
            key,
            self.network.generate_msg_id()
        )
        
        response = await self.network.send_message(
            self.node_address,
            msg,
            wait_response=True,
            timeout=5.0
        )
        
        if response and response.msg_type == MessageType.GET_REPLY:
            value = response.data.get('value')
            version = response.data.get('version')
            error = response.data.get('error')
            
            if error:
                print(f"✗ GET failed: {error}")
                return None
            elif value is not None:
                version_str = f" (version: {version})" if version else ""
                print(f"✓ GET {key} = {value}{version_str}")
                return value
            else:
                print(f"✗ Key '{key}' not found")
                return None
        else:
            print(f"✗ GET failed: No response")
            return None
    
    async def get_all_keys(self, node_address: str = None) -> dict:
        """
        Get all keys stored on a specific node.
        
        Args:
            node_address: Node to query (default: connected node)
            
        Returns:
            Dictionary of keys and values
        """
        from communication.message import Message
        
        target = node_address or self.node_address
        
        msg = Message(
            msg_type=MessageType.GET_ALL_KEYS,
            sender_id=self.client_id,
            sender_address="client",
            msg_id=self.network.generate_msg_id(),
            data={}
        )
        
        response = await self.network.send_message(
            target,
            msg,
            wait_response=True,
            timeout=10.0  # Increased timeout for querying keys
        )
        
        if response and response.msg_type == MessageType.GET_ALL_KEYS_REPLY:
            keys_data = response.data.get('keys', {})
            node_id = response.data.get('node_id')
            address = response.data.get('address')
            
            print(f"\n{'='*60}")
            print(f"Keys stored on Node {node_id} ({address})")
            print(f"{'='*60}")
            
            if keys_data:
                print(f"{'Key':<20} {'Hash':<8} {'Value':<20} {'Version'}")
                print(f"{'-'*60}")
                for key, info in keys_data.items():
                    print(f"{key:<20} {info['hash']:<8} {str(info['value']):<20} {info['version']}")
            else:
                print("No keys stored on this node")
            
            print(f"{'='*60}")
            print(f"Total: {len(keys_data)} keys\n")
            
            return keys_data
        else:
            print(f"✗ Failed to get keys from {target}")
            return {}
    
    async def run_interactive(self):
        """Run interactive command-line interface."""
        await self.connect()
        
        print("\n" + "="*60)
        print("Chord DHT Client")
        print("="*60)
        print("Commands:")
        print("  put <key> <value>     - Store a key-value pair")
        print("  get <key>             - Retrieve a value")
        print("  hash <key>            - Show hash of a key")
        print("  ring                  - Show ring topology")
        print("  keys                  - Show all keys on connected node")
        print("  keys <host:port>      - Show all keys on specific node")
        print("  allkeys               - Show keys on all nodes in ring")
        print("  quit                  - Exit")
        print("="*60 + "\n")
        
        try:
            while True:
                try:
                    # Get user input
                    command = input("> ").strip()
                    
                    if not command:
                        continue
                    
                    parts = command.split(maxsplit=2)
                    cmd = parts[0].lower()
                    
                    if cmd == "quit" or cmd == "exit":
                        break
                    
                    elif cmd == "put":
                        if len(parts) < 3:
                            print("Usage: put <key> <value>")
                            continue
                        
                        key = parts[1]
                        value = parts[2]
                        await self.put(key, value)
                    
                    elif cmd == "get":
                        if len(parts) < 2:
                            print("Usage: get <key>")
                            continue
                        
                        key = parts[1]
                        await self.get(key)
                    
                    elif cmd == "hash":
                        if len(parts) < 2:
                            print("Usage: hash <key>")
                            continue
                        key = parts[1]
                        key_hash = hash_key(key, config.M)
                        ring_size = self.ring_info.get('ring_size', 2**config.M) if self.ring_info else 2**config.M
                        print(f"Key '{key}' -> hash={key_hash} (ring space: 0-{ring_size - 1})")
                        
                        # Show ring nodes
                        if self.ring_info and 'ring_nodes' in self.ring_info:
                            nodes = sorted(self.ring_info['ring_nodes'], key=lambda x: x['node_id'])
                            node_strs = [f"{n['node_id']} ({n['address']})" for n in nodes]
                            print(f"Ring nodes: {', '.join(node_strs)}")
                            
                            # Determine responsible node
                            responsible = self.get_responsible_node(key_hash)
                            if responsible:
                                print(f"Key should be stored on Node {responsible['node_id']} ({responsible['address']})")
                        else:
                            print("Ring info not available. Use 'ring' command to refresh.")
                    
                    elif cmd == "keys":
                        if len(parts) >= 2:
                            # Query specific node
                            await self.get_all_keys(parts[1])
                        else:
                            # Query connected node
                            await self.get_all_keys()
                    
                    elif cmd == "allkeys":
                        # Discover all nodes in the ring by traversing
                        print("\nDiscovering all nodes in the ring...")
                        all_nodes = await self.discover_all_nodes()
                        
                        if all_nodes:
                            # Sort by node_id for consistent display
                            all_nodes = sorted(all_nodes, key=lambda x: x['node_id'])
                            print(f"Found {len(all_nodes)} nodes in the ring.\n")
                            for node_info in all_nodes:
                                try:
                                    await self.get_all_keys(node_info['address'])
                                except Exception as e:
                                    print(f"✗ Failed to query {node_info['address']}: {e}")
                        else:
                            print("Could not discover any nodes in the ring.")
                    
                    elif cmd == "ring":
                        # Discover all nodes in the ring by traversing
                        print("\nDiscovering ring topology...")
                        all_nodes = await self.discover_all_nodes()
                        
                        if all_nodes:
                            # Sort by node_id for consistent display
                            all_nodes = sorted(all_nodes, key=lambda x: x['node_id'])
                            
                            m = self.ring_info.get('m', config.M) if self.ring_info else config.M
                            ring_size = self.ring_info.get('ring_size', 2**config.M) if self.ring_info else 2**config.M
                            
                            print(f"\n{'='*60}")
                            print(f"Ring Topology (M={m}, size={ring_size}, nodes={len(all_nodes)})")
                            print(f"{'='*60}")
                            
                            # Query each node individually for its pred/succ info
                            for n in all_nodes:
                                # Try to get full info from each node
                                try:
                                    node_msg = Message(
                                        msg_type=MessageType.GET_RING_INFO,
                                        sender_id=self.client_id,
                                        sender_address="client",
                                        msg_id=self.network.generate_msg_id(),
                                        data={}
                                    )
                                    response = await self.network.send_message(
                                        n['address'], node_msg, wait_response=True, timeout=2.0
                                    )
                                    if response and response.msg_type == MessageType.GET_RING_INFO_REPLY:
                                        node_info = response.data.get('ring_nodes', [{}])[0]
                                        pred = node_info.get('predecessor', {})
                                        succ = node_info.get('successor', {})
                                    else:
                                        pred = n.get('predecessor', {})
                                        succ = n.get('successor', {})
                                except:
                                    pred = n.get('predecessor', {})
                                    succ = n.get('successor', {})
                                
                                pred_str = f"pred={pred.get('node_id', '?')}" if pred else "pred=None"
                                succ_str = f"succ={succ.get('node_id', '?')}" if succ else "succ=None"
                                print(f"  Node {n['node_id']:3d} ({n['address']}) - {pred_str}, {succ_str}")
                            print(f"{'='*60}\n")
                            
                            # Update cached ring_info with all discovered nodes
                            self.ring_info = {
                                'ring_nodes': all_nodes,
                                'ring_size': ring_size,
                                'm': m
                            }
                        else:
                            print("Failed to discover ring topology")
                    
                    elif cmd == "help":
                        print("Commands:")
                        print("  put <key> <value>     - Store a key-value pair")
                        print("  get <key>             - Retrieve a value")
                        print("  hash <key>            - Show hash of a key")
                        print("  ring                  - Show ring topology")
                        print("  keys                  - Show all keys on connected node")
                        print("  keys <host:port>      - Show all keys on specific node")
                        print("  allkeys               - Show keys on all nodes in ring")
                        print("  quit                  - Exit")
                    
                    else:
                        print(f"Unknown command: {cmd}")
                        print("Type 'help' for available commands")
                
                except KeyboardInterrupt:
                    print("\n")
                    break
                except Exception as e:
                    print(f"Error: {e}")
        
        finally:
            await self.disconnect()


async def main():
    """Main entry point for client."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Chord DHT Client')
    parser.add_argument('--node', type=str, default='localhost:5000',
                       help='Chord node address (default: localhost:5000)')
    parser.add_argument('--put', nargs=2, metavar=('KEY', 'VALUE'),
                       help='Put a key-value pair and exit')
    parser.add_argument('--get', type=str, metavar='KEY',
                       help='Get a value and exit')
    
    args = parser.parse_args()
    
    client = ChordClient(args.node)
    
    # Non-interactive mode
    if args.put:
        await client.connect()
        key, value = args.put
        await client.put(key, value)
        await client.disconnect()
    
    elif args.get:
        await client.connect()
        await client.get(args.get)
        await client.disconnect()
    
    # Interactive mode
    else:
        await client.run_interactive()


if __name__ == "__main__":
    asyncio.run(main())
