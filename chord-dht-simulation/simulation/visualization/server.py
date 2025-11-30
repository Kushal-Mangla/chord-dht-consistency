from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
import sys
import os

# Add parent directory to path to import simulation modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from simulation.simulator import Simulator

app = Flask(__name__)
app.config['SECRET_KEY'] = 'chord-dht-secret'
socketio = SocketIO(app, cors_allowed_origins="*")

# Initialize simulator
simulator = Simulator(m=8)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/nodes', methods=['GET'])
def get_nodes():
    """Get all nodes and their states."""
    nodes = simulator.get_nodes()
    return jsonify({'status': 'success', 'nodes': nodes})

@app.route('/api/ring', methods=['GET'])
def get_ring():
    """Get ring visualization data."""
    ring_data = simulator.get_ring_data()
    return jsonify({'status': 'success', 'ring': ring_data})

@app.route('/api/node/join', methods=['POST'])
def join_node():
    """Add a new node to the ring."""
    data = request.json
    node_id = str(data.get('node_id'))
    address = data.get('address')
    
    result = simulator.join_node(node_id, address)
    
    if result:
        # Broadcast update to all clients
        socketio.emit('node_update', {'nodes': simulator.get_nodes()})
        socketio.emit('log_update', {'log': simulator.get_event_log(10)})
        return jsonify({'status': 'success', 'message': f'Node {node_id} joined.'})
    else:
        return jsonify({'status': 'error', 'message': f'Failed to join node {node_id}.'}), 400

@app.route('/api/node/leave', methods=['POST'])
def leave_node():
    """Remove a node from the ring."""
    data = request.json
    node_id = str(data.get('node_id'))
    
    result = simulator.leave_node(node_id)
    
    if result:
        socketio.emit('node_update', {'nodes': simulator.get_nodes()})
        socketio.emit('log_update', {'log': simulator.get_event_log(10)})
        return jsonify({'status': 'success', 'message': f'Node {node_id} left.'})
    else:
        return jsonify({'status': 'error', 'message': f'Failed to remove node {node_id}.'}), 400

@app.route('/api/node/offline', methods=['POST'])
def set_node_offline():
    """Set a node to offline state."""
    data = request.json
    node_id = str(data.get('node_id'))
    
    result = simulator.simulate_offline(node_id)
    
    if result:
        socketio.emit('node_update', {'nodes': simulator.get_nodes()})
        socketio.emit('log_update', {'log': simulator.get_event_log(10)})
        return jsonify({'status': 'success', 'message': f'Node {node_id} is now offline.'})
    else:
        return jsonify({'status': 'error', 'message': f'Failed to set node {node_id} offline.'}), 400

@app.route('/api/node/online', methods=['POST'])
def set_node_online():
    """Bring a node back online."""
    data = request.json
    node_id = str(data.get('node_id'))
    
    result = simulator.simulate_online(node_id)
    
    if result:
        socketio.emit('node_update', {'nodes': simulator.get_nodes()})
        socketio.emit('log_update', {'log': simulator.get_event_log(10)})
        return jsonify({'status': 'success', 'message': f'Node {node_id} is now online.'})
    else:
        return jsonify({'status': 'error', 'message': f'Failed to bring node {node_id} online.'}), 400

@app.route('/api/put', methods=['POST'])
def put_key_value():
    """Store a key-value pair."""
    data = request.json
    key = data.get('key')
    value = data.get('value')
    write_quorum = data.get('write_quorum', 2)
    
    if not key or value is None:
        return jsonify({'status': 'error', 'message': 'Key and value are required.'}), 400
    
    result = simulator.put(key, value, write_quorum)
    
    if result:
        socketio.emit('node_update', {'nodes': simulator.get_nodes()})
        socketio.emit('log_update', {'log': simulator.get_event_log(10)})
        return jsonify({'status': 'success', 'message': f'Key "{key}" stored successfully.'})
    else:
        return jsonify({'status': 'error', 'message': f'Failed to store key "{key}".'}), 400

@app.route('/api/get', methods=['GET'])
def get_value():
    """Retrieve a value by key."""
    key = request.args.get('key')
    read_quorum = int(request.args.get('read_quorum', 2))
    
    if not key:
        return jsonify({'status': 'error', 'message': 'Key is required.'}), 400
    
    value = simulator.get(key, read_quorum)
    
    socketio.emit('log_update', {'log': simulator.get_event_log(10)})
    
    if value is not None:
        return jsonify({'status': 'success', 'key': key, 'value': value})
    else:
        return jsonify({'status': 'error', 'key': key, 'message': 'Key not found.'}), 404

@app.route('/api/events', methods=['GET'])
def get_events():
    """Get recent event log entries."""
    limit = int(request.args.get('limit', 50))
    events = simulator.get_event_log(limit)
    return jsonify({'status': 'success', 'events': events})

@app.route('/api/events/clear', methods=['POST'])
def clear_events():
    """Clear the event log."""
    simulator.reset_event_log()
    socketio.emit('log_update', {'log': []})
    return jsonify({'status': 'success', 'message': 'Event log cleared.'})

# WebSocket events
@socketio.on('connect')
def handle_connect():
    """Handle client connection."""
    emit('node_update', {'nodes': simulator.get_nodes()})
    emit('log_update', {'log': simulator.get_event_log(20)})

@socketio.on('request_update')
def handle_update_request():
    """Handle manual update request from client."""
    emit('node_update', {'nodes': simulator.get_nodes()})
    emit('log_update', {'log': simulator.get_event_log(20)})

if __name__ == '__main__':
    print("Starting Chord DHT Simulation Server...")
    print("Access the simulation at: http://localhost:5000")
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)