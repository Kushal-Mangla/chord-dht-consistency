"""
Main entry point for Chord DHT node.
"""

import asyncio
import argparse
import logging
import sys
from chord.node import ChordNode
from chord.routing import NodeInfo
from consistency.quorum import QuorumManager
from communication.network import NetworkManager
from communication.message import MessageType
import config


async def run_node(args):
    """
    Run a Chord node.
    
    Args:
        args: Command-line arguments
    """
    # Setup logging
    log_level = getattr(logging, args.log_level.upper())
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger("Main")
    
    # Determine the advertised address (what other nodes use to connect to us)
    # If host is 0.0.0.0, try to get the actual IP address
    advertised_host = args.host
    if args.host == '0.0.0.0':
        import socket
        try:
            # Get the IP address by connecting to an external address
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            advertised_host = s.getsockname()[0]
            s.close()
        except:
            advertised_host = socket.gethostname()
    
    # Create address (this is what we advertise to other nodes)
    address = f"{advertised_host}:{args.port}"
    
    # Create Chord node
    logger.info(f"Creating Chord node at {address}")
    node = ChordNode(
        address=address,
        m=args.m,
        n_replicas=args.N
    )
    
    logger.info(f"Node ID: {node.node_id}")
    logger.info(f"Quorum config: N={args.N}, R={args.R}, W={args.W}")
    
    # Create network manager (bind to the specified host, which can be 0.0.0.0)
    network = NetworkManager(node.node_id, args.host, args.port)
    
    # Create quorum manager with network manager
    quorum_mgr = QuorumManager(
        node_id=node.node_id,
        n_replicas=args.N,
        read_quorum=args.R,
        write_quorum=args.W,
        enable_read_repair=True,
        network_manager=network
    )
    
    logger.info(f"Consistency level: {quorum_mgr.get_consistency_level()}")
    
    # Helper function to find the truly responsible node via network queries
    async def find_responsible_node(key_hash: int, max_hops: int = 10) -> NodeInfo:
        """Find the node responsible for a key hash, following the ring."""
        current = node.find_successor(key_hash)
        
        if not current or current.node_id == node.node_id:
            return node.get_info()
        
        # Follow the ring until we find the actual responsible node
        visited = {node.node_id}
        hops = 0
        
        while hops < max_hops and current.node_id not in visited:
            visited.add(current.node_id)
            
            # Ask this node who is responsible for the key
            from communication.message import Message
            msg = Message(
                msg_type=MessageType.FIND_SUCCESSOR,
                sender_id=node.node_id,
                sender_address=node.address,
                msg_id=network.generate_msg_id(),
                data={'identifier': key_hash}
            )
            
            response = await network.send_message(
                current.address, msg, wait_response=True, timeout=3.0
            )
            
            if response and response.msg_type == MessageType.FIND_SUCCESSOR_REPLY:
                succ_data = response.data.get('successor')
                if succ_data:
                    next_node = NodeInfo(succ_data['node_id'], succ_data['address'])
                    if next_node.node_id == current.node_id:
                        # This node says it's responsible
                        return current
                    current = next_node
                else:
                    return current
            else:
                # Can't reach node, return what we have
                return current
            
            hops += 1
        
        return current
    
    # Register message handlers
    async def handle_put(msg):
        """Handle client PUT request with quorum replication."""
        from communication.message import create_reply_msg, Message
        from chord.storage import hash_key
        from consistency.vector_clock import VectorClock
        
        try:
            key = msg.data['key']
            value = msg.data['value']
            key_hash = hash_key(key, args.m)
            
            logger.info(f"PUT: key='{key}' (hash={key_hash}) value='{value}'")
            
            # Find the node responsible for this key
            responsible_node = await find_responsible_node(key_hash)
            logger.info(f"Responsible node for hash={key_hash}: {responsible_node}")
            
            # Track whether the primary node is available
            primary_node_id = responsible_node.node_id if responsible_node else node.node_id
            is_primary_available = True
            
            # If we're not the responsible node, forward the request
            if responsible_node and responsible_node.node_id != node.node_id:
                logger.info(f"Forwarding PUT to responsible node: {responsible_node}")
                # Forward to the correct node
                forward_msg = Message(
                    msg_type=MessageType.PUT,
                    sender_id=node.node_id,
                    sender_address=node.address,
                    msg_id=msg.msg_id,
                    data=msg.data
                )
                try:
                    response = await network.send_message(
                        responsible_node.address, forward_msg, wait_response=True, timeout=5.0
                    )
                    if response:
                        return response
                    else:
                        logger.warning(f"Forward to {responsible_node} failed, using sloppy quorum")
                        is_primary_available = False
                        # Fall through to sloppy quorum
                except Exception as e:
                    logger.warning(f"Forward to {responsible_node} failed with error: {e}, using sloppy quorum")
                    is_primary_available = False
                    # Fall through to sloppy quorum
            
            # Handle storage based on whether we're primary or using sloppy quorum
            if is_primary_available and responsible_node.node_id == node.node_id:
                # We ARE the responsible/primary node - store as primary
                version = node.storage.put(key, value)
                logger.info(f"Stored {key}={value} locally as PRIMARY with version {version}")
            else:
                # Primary node is down - use sloppy quorum (store as backup with hint)
                # Check if we already have a backup for this key and increment from existing version
                existing_backup = node.storage.get_backup(key, for_node_id=primary_node_id)
                if existing_backup:
                    _, existing_version = existing_backup
                    version = existing_version.copy()
                    version.increment(node.node_id)
                    logger.info(f"Updating existing backup: {key}={value} with incremented version {version}")
                else:
                    version = VectorClock()
                    version.increment(node.node_id)
                    logger.info(f"Creating new backup: {key}={value} with version {version}")
                
                node.storage.put_backup(key, value, version, for_node_id=primary_node_id)
                logger.info(f"Stored {key}={value} as BACKUP for node {primary_node_id} (sloppy quorum) with version {version}")
            
            # NEW: Use full ring knowledge to get N replicas from the key hash
            # This ensures we get the CORRECT replicas even if primary is down
            all_replicas = node.finger_table.get_n_successors(key_hash, args.N)
            
            # Filter out the primary node and ourselves
            replicas = []
            for r in all_replicas:
                if r.node_id != node.node_id and r.node_id != primary_node_id:
                    replicas.append(r)
                if len(replicas) >= args.N - 1:
                    break
            
            # Fallback to old successor list if finger table doesn't have full knowledge yet
            if len(replicas) == 0 and len(node.successor_list) > 0:
                replicas = node.get_successor_list()[:args.N - 1]
            
            logger.info(f"Using {len(replicas)} replica(s) for replication (from full ring knowledge)")
            logger.info(f"Replicating to: {[str(r) for r in replicas]}")
            
            # We already have 1 ack (local store), so we need W-1 more from replicas
            needed_remote_acks = max(0, args.W - 1)
            
            if needed_remote_acks == 0:
                # Local store is enough (W=1)
                success = True
                final_version = version
                logger.info(f"W=1, local store is sufficient")
            elif len(replicas) == 0:
                # No replicas available - STRICT: fail if we need more acks
                logger.error(f"No replicas available! Successor list is empty.")
                logger.error(f"Finger table successor: {node.finger_table.get_successor()}")
                success = False
                final_version = version
                logger.error(f"STRICT MODE: PUT failed - need {needed_remote_acks} replica acks but no replicas available")
            else:
                # Perform quorum write to replicas
                # We need at least needed_remote_acks from replicas
                logger.info(f"Need {needed_remote_acks} remote ack(s) from {len(replicas)} replica(s)")
                logger.info(f"Primary node ID for replication: {primary_node_id}")
                
                # Temporarily adjust write quorum
                original_w = quorum_mgr.write_quorum
                quorum_mgr.write_quorum = needed_remote_acks
                
                # Pass primary_node_id so replicas know which node is the real primary
                success, final_version = await quorum_mgr.quorum_put(
                    key, value, version, replicas, primary_node_id=primary_node_id
                )
                
                quorum_mgr.write_quorum = original_w  # Restore
                
                if not success:
                    # STRICT MODE: Quorum failed, report failure
                    logger.error(f"STRICT MODE: Quorum write failed - could not get {needed_remote_acks} acks from replicas")
            
            if success:
                return create_reply_msg(
                    node.node_id,
                    node.address,
                    MessageType.PUT_REPLY,
                    {'status': 'ok'},
                    msg.msg_id
                )
            else:
                error_msg = f"Quorum write failed: could not reach {args.W} replicas"
                logger.error(error_msg)
                return create_reply_msg(
                    node.node_id,
                    node.address,
                    MessageType.PUT_REPLY,
                    {'status': 'error', 'error': error_msg},
                    msg.msg_id
                )
        except Exception as e:
            import traceback
            logger.error(f"PUT handler exception: {e}\n{traceback.format_exc()}")
            return create_reply_msg(
                node.node_id,
                node.address,
                MessageType.PUT_REPLY,
                {'status': 'error', 'error': str(e)},
                msg.msg_id
            )
    
    async def handle_get(msg):
        """Handle client GET request with quorum reads."""
        from communication.message import create_reply_msg, Message
        from chord.storage import hash_key
        
        key = msg.data['key']
        key_hash = hash_key(key, args.m)
        
        logger.info(f"GET: key='{key}' (hash={key_hash})")
        
        # Find the node responsible for this key
        responsible_node = await find_responsible_node(key_hash)
        logger.info(f"Responsible node for hash={key_hash}: {responsible_node}")
        
        # If we're not the responsible node, forward the request
        if responsible_node and responsible_node.node_id != node.node_id:
            logger.info(f"Forwarding GET to responsible node: {responsible_node}")
            forward_msg = Message(
                msg_type=MessageType.GET,
                sender_id=node.node_id,
                sender_address=node.address,
                msg_id=msg.msg_id,
                data=msg.data
            )
            try:
                response = await network.send_message(
                    responsible_node.address, forward_msg, wait_response=True, timeout=5.0
                )
                if response:
                    return response
                else:
                    logger.warning(f"Forward to {responsible_node} failed, checking locally as fallback")
                    # Fall through to check locally as fallback
            except Exception as e:
                logger.warning(f"Forward to {responsible_node} failed with error: {e}, checking locally as fallback")
                # Fall through to check locally as fallback
        
        # We are responsible (or forwarding failed) - check local storage
        key = msg.data['key']
        
        # Try local first
        local_result = node.storage.get(key)
        
        if local_result:
            value, version = local_result
            
            # If R=1, local is sufficient
            if args.R <= 1:
                logger.info(f"Local GET {key}={value} with version {version}")
                return create_reply_msg(
                    node.node_id,
                    node.address,
                    MessageType.GET_REPLY,
                    {'value': value, 'version': version.to_dict()},
                    msg.msg_id
                )
            
            # For R>1, we need to query replicas too for consistency
            # But we already have 1 read (local), so adjust read quorum
            # NEW: Use full ring knowledge to get replicas
            key_hash = hash_key(key, args.m)
            all_replicas = node.finger_table.get_n_successors(key_hash, args.N)
            replicas = [r for r in all_replicas[1:args.N] if r.node_id != node.node_id]
            
            # Fallback to old successor list if finger table doesn't have full knowledge yet
            if len(replicas) == 0 and len(node.successor_list) > 0:
                replicas = node.get_successor_list()[:args.N - 1]
            
            needed_remote_reads = args.R - 1  # We have 1 local read
            
            if len(replicas) == 0 and needed_remote_reads > 0:
                # STRICT MODE: No replicas available but we need more reads
                logger.error(f"STRICT MODE: GET failed - need {needed_remote_reads} replica reads but no replicas available")
                return create_reply_msg(
                    node.node_id,
                    node.address,
                    MessageType.GET_REPLY,
                    {'value': None, 'error': f'Quorum read failed: need {args.R} reads but only 1 available'},
                    msg.msg_id
                )
            
            if needed_remote_reads <= 0:
                # We have enough reads locally
                logger.info(f"Local read sufficient for R={args.R}")
                return create_reply_msg(
                    node.node_id,
                    node.address,
                    MessageType.GET_REPLY,
                    {'value': value, 'version': version.to_dict()},
                    msg.msg_id
                )
            
            # Temporarily adjust read quorum
            original_r = quorum_mgr.read_quorum
            quorum_mgr.read_quorum = needed_remote_reads
            
            # Get primary node ID for the key (in case replicas have it as backup)
            responsible_for_key = await find_responsible_node(key_hash)
            primary_node_id_for_key = responsible_for_key.node_id if responsible_for_key else node.node_id
            
            result = await quorum_mgr.quorum_get(key, replicas, primary_node_id=primary_node_id_for_key)
            
            quorum_mgr.read_quorum = original_r
            
            if result:
                remote_value, remote_version = result
                # Compare versions and return the latest
                from consistency.vector_clock import get_latest_version
                latest = get_latest_version([version, remote_version])
                if latest == remote_version:
                    value, version = remote_value, remote_version
                
                logger.info(f"Quorum GET {key}={value} with version {version}")
                return create_reply_msg(
                    node.node_id,
                    node.address,
                    MessageType.GET_REPLY,
                    {'value': value, 'version': version.to_dict()},
                    msg.msg_id
                )
            else:
                # STRICT MODE: Replica query failed
                logger.error(f"STRICT MODE: Quorum read failed - could not get {needed_remote_reads} reads from replicas")
                return create_reply_msg(
                    node.node_id,
                    node.address,
                    MessageType.GET_REPLY,
                    {'value': None, 'error': f'Quorum read failed: could not reach {args.R} replicas'},
                    msg.msg_id
                )
        else:
            # Not found locally in primary storage
            # This could mean:
            # 1. We're not the responsible node (shouldn't happen, we already forwarded)
            # 2. The key was stored with sloppy quorum (primary was down)
            # 3. The key doesn't exist
            
            logger.info(f"Key {key} not found in primary storage")
            
            # First, check if we have it as a backup (sloppy quorum)
            # Find the actual responsible node for this key
            responsible_node_for_key = await find_responsible_node(key_hash)
            if responsible_node_for_key:
                primary_node_id = responsible_node_for_key.node_id
                
                # Check our backup storage for this primary node
                backup_result = node.storage.get_backup(key, for_node_id=primary_node_id)
                if backup_result:
                    value, version = backup_result
                    logger.info(f"Found {key}={value} in backup for node {primary_node_id} (sloppy quorum)")
                    
                    # For R=1, this is sufficient
                    if args.R <= 1:
                        return create_reply_msg(
                            node.node_id,
                            node.address,
                            MessageType.GET_REPLY,
                            {'value': value, 'version': version.to_dict()},
                            msg.msg_id
                        )
                    
                    # For R>1, we have 1 read, need more from other replicas
                    # Try to get from other nodes that might have backups
                    all_replicas = node.finger_table.get_n_successors(key_hash, args.N)
                    replicas = [r for r in all_replicas if r.node_id != node.node_id and r.node_id != primary_node_id]
                    
                    if len(replicas) == 0 and len(node.successor_list) > 0:
                        replicas = [r for r in node.get_successor_list() if r.node_id != node.node_id]
                    
                    if len(replicas) > 0:
                        # Query replicas for this key (they might have it as backup too)
                        result = await quorum_mgr.quorum_get(key, replicas[:args.R-1], primary_node_id=primary_node_id)
                        if result:
                            remote_value, remote_version = result
                            from consistency.vector_clock import get_latest_version
                            latest = get_latest_version([version, remote_version])
                            if latest == remote_version:
                                value, version = remote_value, remote_version
                    
                    return create_reply_msg(
                        node.node_id,
                        node.address,
                        MessageType.GET_REPLY,
                        {'value': value, 'version': version.to_dict()},
                        msg.msg_id
                    )
            
            # Not in our backups either, try querying replicas
            logger.info(f"Key {key} not in backups, querying replicas")
            all_replicas = node.finger_table.get_n_successors(key_hash, args.N)
            replicas = [r for r in all_replicas if r.node_id != node.node_id][:args.N - 1]
            
            if len(replicas) == 0 and len(node.successor_list) > 0:
                replicas = node.get_successor_list()[:args.N - 1]
            
            # Get primary node ID hint for checking backups
            responsible_for_key = await find_responsible_node(key_hash)
            primary_node_id_for_key = responsible_for_key.node_id if responsible_for_key else node.node_id
            
            result = await quorum_mgr.quorum_get(key, replicas, primary_node_id=primary_node_id_for_key)
            
            if result:
                value, version = result
                logger.info(f"Quorum GET from replicas {key}={value} with version {version}")
                return create_reply_msg(
                    node.node_id,
                    node.address,
                    MessageType.GET_REPLY,
                    {'value': value, 'version': version.to_dict()},
                    msg.msg_id
                )
            else:
                logger.warning(f"Key {key} not found")
                return create_reply_msg(
                    node.node_id,
                    node.address,
                    MessageType.GET_REPLY,
                    {'value': None},
                    msg.msg_id
                )
    
    async def handle_put_replica(msg):
        """Handle PUT_REPLICA request from another node."""
        from communication.message import create_reply_msg
        from chord.storage import hash_key
        
        logger.info(f"PUT_REPLICA received from {msg.sender_id} ({msg.sender_address})")
        
        try:
            key = msg.data['key']
            value = msg.data['value']
            version_dict = msg.data['version']
            key_hash = hash_key(key, args.m)
            
            # Get the primary node ID (from message data, defaults to sender if not provided)
            # This is crucial for sloppy quorum / hinted handoff
            primary_node_id = msg.data.get('primary_node_id', msg.sender_id)
            
            # Reconstruct vector clock
            from consistency.vector_clock import VectorClock
            incoming_version = VectorClock.from_dict(version_dict)
            
            # Check if we already have a backup for this key
            existing_backup = node.storage.get_backup(key, for_node_id=primary_node_id)
            if existing_backup:
                _, existing_version = existing_backup
                # Merge with incoming version and increment
                version = existing_version.copy()
                version.update(incoming_version)
                version.increment(node.node_id)
                logger.info(f"Merging backup versions: existing={existing_version}, incoming={incoming_version}, result={version}")
            else:
                # No existing backup, use incoming version and increment
                version = incoming_version.copy()
                version.increment(node.node_id)
            
            # Store as BACKUP for the primary node
            # If primary_node_id is the sender, they're the actual primary
            # If primary_node_id is different, this is sloppy quorum (primary is down)
            node.storage.put_backup(key, value, version, for_node_id=primary_node_id)
            
            if primary_node_id == msg.sender_id:
                logger.info(f"BACKUP STORED: key='{key}' value='{value}' for PRIMARY node {primary_node_id}")
            else:
                logger.info(f"BACKUP STORED (sloppy quorum): key='{key}' value='{value}' for PRIMARY node {primary_node_id} (currently down, hint from {msg.sender_id})")
            
            return create_reply_msg(
                node.node_id,
                node.address,
                MessageType.PUT_REPLICA_REPLY,
                {'status': 'ok', 'version': version.to_dict()},
                msg.msg_id
            )
        except Exception as e:
            logger.error(f"PUT_REPLICA handler error: {e}")
            import traceback
            traceback.print_exc()
            return create_reply_msg(
                node.node_id,
                node.address,
                MessageType.PUT_REPLICA_REPLY,
                {'status': 'error', 'error': str(e)},
                msg.msg_id
            )
    
    async def handle_get_replica(msg):
        """Handle GET_REPLICA request from another node."""
        from communication.message import create_reply_msg
        from chord.storage import hash_key
        
        key = msg.data['key']
        primary_node_id_hint = msg.data.get('primary_node_id')  # Optional hint
        
        # Try to get value from primary store first
        value, version = node.storage.get_with_version(key)
        
        # If not in primary store, check if we have it as a backup
        if value is None:
            # If we have a hint about which node is primary, check that backup first
            if primary_node_id_hint is not None:
                backup = node.storage.get_backup(key, for_node_id=primary_node_id_hint)
                if backup:
                    value, version = backup
                    logger.debug(f"GET_REPLICA {key}={value} from backup for node {primary_node_id_hint}")
            
            # If still not found, check all backup stores
            if value is None:
                for node_id, backups in node.storage.backup_store.items():
                    if key in backups:
                        value, version = backups[key]
                        logger.debug(f"GET_REPLICA {key}={value} from backup (for node {node_id})")
                        break
        
        if value is not None:
            logger.debug(f"GET_REPLICA {key}={value} with version {version}")
        else:
            logger.debug(f"GET_REPLICA {key} not found")
        
        return create_reply_msg(
            node.node_id,
            node.address,
            MessageType.GET_REPLICA_REPLY,
            {
                'value': value,
                'version': version.to_dict() if version else None
            },
            msg.msg_id
        )
    
    async def handle_get_all_keys(msg):
        """Handle GET_ALL_KEYS request - return all stored keys and values."""
        from communication.message import create_reply_msg
        from chord.storage import hash_key
        
        all_keys = node.storage.get_all_keys()
        data = {}
        for key in all_keys:
            value, version = node.storage.get_with_version(key)
            key_hash = hash_key(key, args.m)
            data[key] = {
                'value': value,
                'hash': key_hash,
                'version': str(version) if version else None
            }
        
        logger.info(f"GET_ALL_KEYS: returning {len(data)} keys")
        
        return create_reply_msg(
            node.node_id,
            node.address,
            MessageType.GET_ALL_KEYS_REPLY,
            {'keys': data, 'node_id': node.node_id, 'address': node.address},
            msg.msg_id
        )
    
    async def handle_get_ring_info(msg):
        """Handle GET_RING_INFO request - return ring topology info."""
        from communication.message import create_reply_msg
        
        ring_nodes = []
        
        # Add self with full info
        ring_nodes.append({
            'node_id': node.node_id,
            'address': node.address,
            'predecessor': {'node_id': node.predecessor.node_id, 'address': node.predecessor.address} if node.predecessor else None,
            'successor': {'node_id': node.finger_table.get_successor().node_id, 'address': node.finger_table.get_successor().address} if node.finger_table.get_successor() else None
        })
        
        # Add nodes from successor list (basic info only, no network calls)
        seen = {node.node_id}
        for succ in node.successor_list:
            if succ.node_id not in seen:
                ring_nodes.append({
                    'node_id': succ.node_id,
                    'address': succ.address,
                    'predecessor': None,
                    'successor': None
                })
                seen.add(succ.node_id)
        
        # Also add predecessor if not already in list
        if node.predecessor and node.predecessor.node_id not in seen:
            ring_nodes.append({
                'node_id': node.predecessor.node_id,
                'address': node.predecessor.address,
                'predecessor': None,
                'successor': None
            })
        
        logger.info(f"GET_RING_INFO: returning {len(ring_nodes)} nodes")
        
        return create_reply_msg(
            node.node_id,
            node.address,
            MessageType.GET_RING_INFO_REPLY,
            {
                'ring_nodes': ring_nodes,
                'ring_size': 2 ** args.m,
                'm': args.m
            },
            msg.msg_id
        )
    
    network.register_handler(MessageType.PUT, handle_put)
    network.register_handler(MessageType.GET, handle_get)
    network.register_handler(MessageType.PUT_REPLICA, handle_put_replica)
    network.register_handler(MessageType.GET_REPLICA, handle_get_replica)
    network.register_handler(MessageType.GET_ALL_KEYS, handle_get_all_keys)
    network.register_handler(MessageType.GET_RING_INFO, handle_get_ring_info)
    
    # Register Chord protocol handlers
    async def handle_find_successor(msg):
        """Handle FIND_SUCCESSOR request."""
        from communication.message import create_reply_msg
        identifier = msg.data.get('identifier')
        successor = node.find_successor(identifier)
        logger.info(f"FIND_SUCCESSOR for id={identifier} -> {successor}")
        
        return create_reply_msg(
            node.node_id,
            node.address,
            MessageType.FIND_SUCCESSOR_REPLY,
            {'successor': {'node_id': successor.node_id, 'address': successor.address} if successor else None},
            msg.msg_id
        )
    
    async def handle_get_predecessor(msg):
        """Handle GET_PREDECESSOR request."""
        from communication.message import create_reply_msg
        pred = node.predecessor
        logger.info(f"GET_PREDECESSOR -> {pred}")
        
        return create_reply_msg(
            node.node_id,
            node.address,
            MessageType.GET_PREDECESSOR_REPLY,
            {'predecessor': {'node_id': pred.node_id, 'address': pred.address} if pred else None},
            msg.msg_id
        )
    
    async def handle_get_successor_list(msg):
        """Handle GET_SUCCESSOR_LIST request."""
        from communication.message import create_reply_msg
        succ_list = node.get_successor_list()
        # Filter out self-references when responding to other nodes
        filtered_list = [n for n in succ_list if n.node_id != node.node_id]
        logger.info(f"GET_SUCCESSOR_LIST -> {[str(n) for n in filtered_list]}")
        
        return create_reply_msg(
            node.node_id,
            node.address,
            MessageType.GET_SUCCESSOR_LIST_REPLY,
            {'successor_list': [{'node_id': n.node_id, 'address': n.address} for n in filtered_list]},
            msg.msg_id
        )
    
    async def handle_notify(msg):
        """Handle NOTIFY message from a node thinking it's our predecessor."""
        notifier = NodeInfo(msg.data['node_id'], msg.data['address'])
        logger.info(f"NOTIFY from {notifier}")
        node.notify(notifier)
        # NOTIFY is fire-and-forget, don't send response
        return None
    
    network.register_handler(MessageType.FIND_SUCCESSOR, handle_find_successor)
    network.register_handler(MessageType.GET_PREDECESSOR, handle_get_predecessor)
    network.register_handler(MessageType.GET_SUCCESSOR_LIST, handle_get_successor_list)
    network.register_handler(MessageType.NOTIFY, handle_notify)
    
    # Register NEW enhanced protocol handlers
    async def handle_get_all_nodes(msg):
        """Handle GET_ALL_NODES request - return list of all known nodes."""
        from communication.message import create_reply_msg
        
        # Get all nodes from finger table
        all_nodes = node.finger_table.get_all_nodes()
        
        # Also add ourselves, predecessor, and successor list
        nodes_set = {n.node_id: n for n in all_nodes}
        nodes_set[node.node_id] = node.get_info()
        
        if node.predecessor:
            nodes_set[node.predecessor.node_id] = node.predecessor
        
        for n in node.successor_list:
            if n.node_id not in nodes_set:
                nodes_set[n.node_id] = n
        
        nodes_list = [{'node_id': n.node_id, 'address': n.address} for n in nodes_set.values()]
        
        logger.info(f"GET_ALL_NODES: returning {len(nodes_list)} nodes")
        
        return create_reply_msg(
            node.node_id,
            node.address,
            MessageType.GET_ALL_NODES_REPLY,
            {'nodes': nodes_list},
            msg.msg_id
        )
    
    async def handle_broadcast_join(msg):
        """Handle BROADCAST_JOIN - a new node is joining the ring."""
        from communication.message import create_reply_msg
        
        new_node = NodeInfo(msg.data['node_id'], msg.data['address'])
        logger.info(f"BROADCAST_JOIN: new node {new_node} joining ring")
        
        # Add to our finger table
        node.finger_table.add_node(new_node)
        
        # Also notify the node that we exist (mutual awareness)
        logger.info(f"Added {new_node} to finger table, now have {len(node.finger_table.get_all_nodes())} nodes")
        
        return create_reply_msg(
            node.node_id,
            node.address,
            MessageType.BROADCAST_JOIN_ACK,
            {'status': 'ok'},
            msg.msg_id
        )
    
    async def handle_transfer_keys_request(msg):
        """Handle TRANSFER_KEYS_REQUEST - send keys that belong to the new node."""
        from communication.message import create_reply_msg
        from chord.storage import hash_key
        
        new_node_id = msg.data['new_node_id']
        predecessor_id = msg.data.get('predecessor_id')
        
        logger.info(f"TRANSFER_KEYS_REQUEST from new node {new_node_id}")
        
        # Determine which keys to transfer
        # Keys belong to new node if their hash is between predecessor_id and new_node_id
        keys_to_transfer = {}
        
        for key in node.storage.get_all_keys():
            key_hash = hash_key(key, args.m)
            
            # Check if this key should belong to the new node
            if predecessor_id is not None:
                from chord.routing import in_range
                if in_range(key_hash, predecessor_id, new_node_id, inclusive_start=False, inclusive_end=True):
                    value, version = node.storage.get(key)
                    keys_to_transfer[key] = {
                        'value': value,
                        'version': version.to_dict()
                    }
            else:
                # No predecessor info, check if key is closer to new node
                # This is a simplified heuristic
                if key_hash <= new_node_id or key_hash > node.node_id:
                    value, version = node.storage.get(key)
                    keys_to_transfer[key] = {
                        'value': value,
                        'version': version.to_dict()
                    }
        
        logger.info(f"Transferring {len(keys_to_transfer)} keys to new node {new_node_id}")
        
        return create_reply_msg(
            node.node_id,
            node.address,
            MessageType.TRANSFER_KEYS_RESPONSE,
            {'keys': keys_to_transfer},
            msg.msg_id
        )
    
    async def handle_ping(msg):
        """Handle PING request - respond with PONG."""
        from communication.message import create_reply_msg
        
        return create_reply_msg(
            node.node_id,
            node.address,
            MessageType.PONG,
            {'status': 'alive'},
            msg.msg_id
        )
    
    async def handle_recover_handoff(msg):
        """
        Handle RECOVER_HANDOFF request from a rejoining node.
        
        When a node comes back online, it requests all keys we stored as backups for it.
        We send those keys back and then delete them from our backup storage.
        """
        from communication.message import create_reply_msg
        
        requesting_node_id = msg.data.get('requesting_node_id', msg.sender_id)
        logger.info(f"RECOVER_HANDOFF: Node {requesting_node_id} requesting hinted handoff data")
        
        # Get all backup keys for this node
        backup_keys = node.storage.get_all_backups_for_node(requesting_node_id)
        
        # Prepare response data
        keys_data = {}
        for key, (value, version) in backup_keys.items():
            keys_data[key] = {
                'value': value,
                'version': version.to_dict()
            }
        
        logger.info(f"Sending {len(keys_data)} hinted handoff keys to node {requesting_node_id}")
        
        # Delete the backups from our storage (handoff complete)
        for key in backup_keys.keys():
            node.storage.delete_backup(key, requesting_node_id)
            logger.debug(f"Deleted backup hint for key '{key}' (node {requesting_node_id})")
        
        return create_reply_msg(
            node.node_id,
            node.address,
            MessageType.RECOVER_HANDOFF_REPLY,
            {'keys': keys_data},
            msg.msg_id
        )
    
    async def handle_update_backup(msg):
        """
        Handle UPDATE_BACKUP request - update or create a backup replica.
        
        This is used when a node recovers and wants to update backup copies
        on its successors with the latest values.
        """
        from communication.message import create_reply_msg
        from consistency.vector_clock import VectorClock
        
        try:
            key = msg.data['key']
            value = msg.data['value']
            version_dict = msg.data['version']
            primary_node_id = msg.data.get('primary_node_id', msg.sender_id)
            
            version = VectorClock.from_dict(version_dict)
            
            # Update or create backup
            node.storage.put_backup(key, value, version, for_node_id=primary_node_id)
            logger.info(f"BACKUP UPDATED: key='{key}' value='{value}' for primary node {primary_node_id}")
            
            return create_reply_msg(
                node.node_id,
                node.address,
                MessageType.UPDATE_BACKUP_ACK,
                {'status': 'ok'},
                msg.msg_id
            )
        except Exception as e:
            logger.error(f"UPDATE_BACKUP handler error: {e}")
            return create_reply_msg(
                node.node_id,
                node.address,
                MessageType.UPDATE_BACKUP_ACK,
                {'status': 'error', 'error': str(e)},
                msg.msg_id
            )
    
    network.register_handler(MessageType.GET_ALL_NODES, handle_get_all_nodes)
    network.register_handler(MessageType.BROADCAST_JOIN, handle_broadcast_join)
    network.register_handler(MessageType.TRANSFER_KEYS_REQUEST, handle_transfer_keys_request)
    network.register_handler(MessageType.PING, handle_ping)
    network.register_handler(MessageType.RECOVER_HANDOFF, handle_recover_handoff)
    network.register_handler(MessageType.UPDATE_BACKUP, handle_update_backup)
    network.register_handler(MessageType.RECOVER_HANDOFF, handle_recover_handoff)
    network.register_handler(MessageType.UPDATE_BACKUP, handle_update_backup)
    
    # Start network server
    await network.start()
    
    # Join the ring
    if args.join:
        logger.info(f"Joining existing ring via {args.join}")
        
        # Parse join address
        join_host, join_port = args.join.split(':')
        from chord.routing import hash_address
        join_node_id = hash_address(args.join, args.m)
        known_node = NodeInfo(join_node_id, args.join)
        
        # Use ENHANCED network-aware join protocol with full ring knowledge
        # This will: get all nodes, broadcast join, transfer keys, load persistent storage
        await node.join_ring_full(known_node, network)
        
        logger.info(f"Joined ring. Successor: {node.finger_table.get_successor()}")
        logger.info(f"Full ring knowledge: {len(node.finger_table.get_all_nodes())} nodes")
        
        # Immediately stabilize and update successor list after joining
        await node.stabilize_network(network)
        await node.update_successor_list_network(network)
        logger.info(f"Initial successor list: {[str(n) for n in node.successor_list]}")
        
        # Run a few more stabilization rounds to ensure ring is formed
        for i in range(3):
            await asyncio.sleep(0.5)
            await node.stabilize_network(network)
            await node.update_successor_list_network(network)
        logger.info(f"After stabilization - Predecessor: {node.predecessor}, Successor: {node.finger_table.get_successor()}")
        
        # RECOVER HINTED HANDOFFS: If this node was previously in the ring and is rejoining,
        # recover keys that were stored as hints on other nodes
        logger.info("Starting hinted handoff recovery...")
        await node.recover_hinted_handoffs(network)
        logger.info("Hinted handoff recovery complete")
    else:
        logger.info("Creating new Chord ring")
        node.create_ring()
        logger.info(f"Ring created. Successor: {node.finger_table.get_successor()}")
    
    logger.info("Node is running. Press Ctrl+C to stop.")
    
    # Network-aware successor list updater
    async def update_successor_list_loop():
        """Periodically update successor list via network queries."""
        while True:
            try:
                await asyncio.sleep(3)  # Update every 3 seconds
                await node.update_successor_list_network(network)
                logger.debug(f"Successor list: {[str(n) for n in node.successor_list]}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Successor list update error: {e}")
    
    # Network-aware stabilization
    async def stabilization_loop():
        while True:
            try:
                await asyncio.sleep(config.STABILIZE_INTERVAL)
                await node.stabilize_network(network)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Stabilization error: {e}")
    
    async def fix_fingers_loop():
        while True:
            try:
                await asyncio.sleep(config.FIX_FINGERS_INTERVAL)
                node.fix_fingers()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Fix fingers error: {e}")
    
    # Create tasks
    tasks = [
        asyncio.create_task(update_successor_list_loop()),
        asyncio.create_task(stabilization_loop()),
        asyncio.create_task(fix_fingers_loop()),
    ]
    
    try:
        # Wait forever (until interrupted)
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        # Cancel tasks
        for task in tasks:
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Stop network
        await network.stop()
        
        logger.info("Node stopped")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Chord DHT Node with Tunable Consistency'
    )
    
    # Node configuration
    parser.add_argument('--host', type=str, default='localhost',
                       help='Host to bind to (default: localhost)')
    parser.add_argument('--port', type=int, default=5000,
                       help='Port to listen on (default: 5000)')
    parser.add_argument('--join', type=str, default=None,
                       help='Address of existing node to join (host:port)')
    
    # Chord parameters
    parser.add_argument('--m', type=int, default=config.M,
                       help=f'Identifier space bits (default: {config.M})')
    
    # Consistency parameters
    parser.add_argument('--N', type=int, default=config.N,
                       help=f'Number of replicas (default: {config.N})')
    parser.add_argument('--R', type=int, default=config.R,
                       help=f'Read quorum size (default: {config.R})')
    parser.add_argument('--W', type=int, default=config.W,
                       help=f'Write quorum size (default: {config.W})')
    
    # Logging
    parser.add_argument('--log-level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level (default: INFO)')
    
    args = parser.parse_args()
    
    # Validate quorum
    if args.R < 1 or args.R > args.N:
        print(f"Error: R must be between 1 and N (got R={args.R}, N={args.N})")
        sys.exit(1)
    
    if args.W < 1 or args.W > args.N:
        print(f"Error: W must be between 1 and N (got W={args.W}, N={args.N})")
        sys.exit(1)
    
    # Print configuration
    print("="*60)
    print("Chord DHT Node with Tunable Consistency")
    print("="*60)
    print(f"Address: {args.host}:{args.port}")
    print(f"Identifier Space: 2^{args.m} = {2**args.m} nodes")
    print(f"Replication: N={args.N}, R={args.R}, W={args.W}")
    
    if args.R + args.W > args.N:
        consistency = "STRONG (R+W > N)"
    elif args.R + args.W == args.N:
        consistency = "MODERATE (R+W = N)"
    else:
        consistency = "EVENTUAL (R+W < N)"
    print(f"Consistency Level: {consistency}")
    
    if args.join:
        print(f"Joining via: {args.join}")
    else:
        print("Creating new ring")
    print("="*60 + "\n")
    
    # Run the node
    try:
        asyncio.run(run_node(args))
    except KeyboardInterrupt:
        print("\nExiting...")


if __name__ == "__main__":
    main()
