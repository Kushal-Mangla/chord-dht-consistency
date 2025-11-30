# Chord DHT Simulation - Quick Start Guide

## Overview

This is an interactive web-based simulation of the Chord Distributed Hash Table (DHT) protocol. It visualizes:
- Node joining and leaving the ring
- Key-value storage with replication
- Quorum-based reads and writes
- Node failures and recovery
- Hinted handoff mechanism

## Features

✅ **Interactive Ring Visualization** - See nodes arranged in a circular DHT structure
✅ **Node Management** - Add, remove, and toggle node states (online/offline)
✅ **Key-Value Operations** - PUT and GET with configurable quorum
✅ **Replication** - Automatic data replication across successor nodes
✅ **Fault Tolerance** - Hinted handoff when nodes go offline
✅ **Real-time Updates** - WebSocket-based live updates
✅ **Event Logging** - Track all operations in real-time

## Quick Start

### Option 1: Using Fish Shell (Recommended for your system)

```fish
cd "/home/mahendrakerarya/codeforces/Distributed_Sys/Ds_courseproject (1)/chord-dht-simulation"
chmod +x run_simulation.fish
./run_simulation.fish
```

### Option 2: Using Bash

```bash
cd "/home/mahendrakerarya/codeforces/Distributed_Sys/Ds_courseproject (1)/chord-dht-simulation"
chmod +x run_simulation.sh
./run_simulation.sh
```

### Option 3: Manual Setup

```fish
cd "/home/mahendrakerarya/codeforces/Distributed_Sys/Ds_courseproject (1)/chord-dht-simulation"

# Create virtual environment
python3 -m venv venv
source venv/bin/activate.fish

# Install dependencies
pip install -r requirements.txt

# Run the server
cd simulation/visualization
python server.py
```

## Access the Simulation

Once the server is running, open your browser and navigate to:

**http://localhost:5000**

## How to Use

### 1. Add Nodes to the Ring

- Enter a node ID (0-255) in the "Node ID" input field
- Click "Add Node" button
- The node will appear in the ring visualization and the nodes list

**Try this:** Add nodes 0, 64, 128, and 192 to create an evenly distributed ring

### 2. Store Key-Value Pairs

- Enter a key in the "Key" field (e.g., "username")
- Enter a value in the "Value" field (e.g., "alice")
- Click "PUT" button
- The system will automatically:
  - Hash the key to find the responsible node
  - Store replicas on successor nodes
  - Show which nodes stored the data in the event log

### 3. Retrieve Values

- Enter a key in the "Key" field
- Click "GET" button
- The system will:
  - Read from multiple replicas
  - Apply quorum consensus
  - Display the value in the Value field

### 4. Simulate Node Failures

- Click on a node in the nodes list to select it
- Click "Set Offline" to simulate the node going offline
- The node will turn red in the visualization
- Try to PUT/GET data to see how the system handles failures

### 5. Recover Nodes

- Select an offline node
- Click "Set Online" to bring it back
- Watch the event log for "hinted handoff" messages
- The node will recover missed updates

### 6. Remove Nodes

- Select a node from the list
- Click "Remove Selected" to permanently remove it
- Data will be redistributed to remaining nodes

## Example Workflow

1. **Setup a ring:**
   ```
   Add nodes: 10, 50, 100, 150, 200
   ```

2. **Store some data:**
   ```
   PUT: key="user1", value="Alice"
   PUT: key="user2", value="Bob"
   PUT: key="data1", value="ImportantData"
   ```

3. **View data distribution:**
   - Click on each node to see which keys it stores
   - Notice that each key is replicated on 3 nodes

4. **Simulate failure:**
   ```
   - Select node 100
   - Click "Set Offline"
   - Try to GET "user1" - should still work due to replication
   ```

5. **Observe recovery:**
   ```
   - Select node 100
   - Click "Set Online"
   - Check event log for hinted handoff recovery
   ```

## Architecture

```
chord-dht-simulation/
├── simulation/
│   ├── simulator.py         # Core Chord DHT logic
│   ├── node_manager.py      # Node management
│   ├── event_manager.py     # Event handling
│   └── visualization/
│       ├── server.py         # Flask web server
│       ├── static/
│       │   ├── css/
│       │   │   └── styles.css
│       │   └── js/
│       │       ├── main.js           # Main application logic
│       │       ├── ring_visualizer.js # Canvas-based ring drawing
│       │       └── event_handler.js  # Event utilities
│       └── templates/
│           ├── index.html
│           └── components/
│               ├── control_panel.html
│               ├── ring_view.html
│               ├── node_details.html
│               └── event_log.html
├── chord/                   # Chord node implementation
├── requirements.txt
└── run_simulation.fish
```

## API Endpoints

The simulation exposes REST API endpoints:

- `GET /api/nodes` - Get all nodes
- `GET /api/ring` - Get ring visualization data
- `POST /api/node/join` - Add a node
- `POST /api/node/leave` - Remove a node
- `POST /api/node/offline` - Set node offline
- `POST /api/node/online` - Bring node online
- `POST /api/put` - Store key-value
- `GET /api/get?key=<key>` - Retrieve value
- `GET /api/events` - Get event log

## Configuration

You can modify the simulation parameters in `simulation/simulator.py`:

```python
self.m = 8  # Hash space size (2^8 = 256 positions)
self.replication_factor = 3  # Number of replicas
```

Default quorum settings:
- Write quorum: 2 (W=2)
- Read quorum: 2 (R=2)

## Troubleshooting

### Port already in use
If port 5000 is busy, edit `simulation/visualization/server.py`:
```python
socketio.run(app, debug=True, host='0.0.0.0', port=5001)
```

### Import errors
Make sure you're in the correct directory and virtual environment is activated:
```fish
cd "/home/mahendrakerarya/codeforces/Distributed_Sys/Ds_courseproject (1)/chord-dht-simulation"
source venv/bin/activate.fish
```

### Canvas not displaying
Check browser console for errors. Try refreshing the page after all nodes are added.

## Technical Details

### Hash Function
- Uses SHA-256 for consistent hashing
- Key space: 0-255 (2^8)
- Nodes and keys are mapped to the same hash space

### Replication Strategy
- Primary node: successor of key hash
- Replicas: next N successor nodes (default N=3)
- Hinted handoff for temporarily offline nodes

### Quorum Reads/Writes
- Configurable R and W values
- Default: R=2, W=2 (out of 3 replicas)
- Ensures consistency when R + W > N

## Future Enhancements

- [ ] Finger table visualization
- [ ] Configurable hash space size
- [ ] Network latency simulation
- [ ] Performance metrics
- [ ] Export/import ring state
- [ ] Batch operations

## License

This is a course project for Distributed Systems.

## Credits

Built for understanding Chord DHT with:
- Replication
- Quorum consensus
- Hinted handoff
- Fault tolerance
