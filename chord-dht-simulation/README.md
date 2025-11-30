# Chord DHT Simulation

A web-based interactive simulation of the Chord Distributed Hash Table (DHT) protocol with visual representation, replication, quorum consistency, and fault tolerance mechanisms.

## 🎯 Features

- **Interactive Ring Visualization** - Real-time canvas-based visualization of the Chord ring
- **Node Management** - Add, remove, and manage node states (online/offline)
- **Key-Value Storage** - PUT/GET operations with quorum-based consistency
- **Replication** - Automatic data replication across 3 successor nodes
- **Fault Tolerance** - Hinted handoff mechanism for offline nodes
- **Real-time Updates** - WebSocket-based live updates for all operations
- **Event Logging** - Comprehensive event tracking and visualization

## 📋 Prerequisites

- Python 3.7 or higher
- pip (Python package manager)
- A modern web browser (Chrome, Firefox, Safari, or Edge)

## 🚀 Quick Start

### Step 1: Navigate to Project Directory

```fish
cd "/home/mahendrakerarya/codeforces/Distributed_Sys/Ds_courseproject (1)/chord-dht-simulation"
```

### Step 2: Create Virtual Environment

```fish
python3 -m venv venv
```

### Step 3: Activate Virtual Environment

**For Fish Shell:**
```fish
source venv/bin/activate.fish
```

**For Bash:**
```bash
source venv/bin/activate
```

### Step 4: Install Dependencies

```fish
pip install -r requirements.txt
```

### Step 5: Run the Simulation Server

```fish
cd simulation/visualization
python server.py
```

### Step 6: Access the Web Interface

Open your browser and navigate to:

```
http://localhost:5000
```

## 🎮 Using the Simulation

### 1. Add Nodes to the Ring

1. Enter a node ID (0-255) in the "Node ID" field
2. Optionally enter an address (e.g., "192.168.1.1")
3. Click **"Add Node"**
4. The node will appear in both the ring visualization and the nodes list

**Example:** Add nodes 0, 64, 128, 192 for an evenly distributed ring

### 2. Store Data (PUT Operation)

1. Enter a key (e.g., "username")
2. Enter a value (e.g., "alice")
3. Click **"PUT"**
4. Watch the event log to see which nodes stored the data

### 3. Retrieve Data (GET Operation)

1. Enter a key in the "Key" field
2. Click **"GET"**
3. The value will appear in the "Value" field
4. Event log shows which nodes were queried

### 4. Simulate Node Failure

1. Click on a node in the nodes list to select it
2. Click **"Set Offline"**
3. The node turns red in the visualization
4. Try PUT/GET operations to see fault tolerance in action

### 5. Recover a Node

1. Select an offline node
2. Click **"Set Online"**
3. Watch for "hinted handoff" messages in the event log
4. The node recovers any missed updates

## 📁 Project Structure

```
chord-dht-simulation/
├── README.md                           # This file
├── QUICKSTART.md                       # Detailed usage guide
├── requirements.txt                    # Python dependencies
├── run_simulation.fish                 # Fish shell script
├── run_simulation.sh                   # Bash shell script
│
├── simulation/
│   ├── simulator.py                    # Core Chord DHT logic
│   └── visualization/
│       ├── server.py                   # Flask + SocketIO server
│       ├── static/
│       │   ├── css/
│       │   │   └── styles.css          # All styling
│       │   └── js/
│       │       ├── main.js             # Main application logic
│       │       ├── ring_visualizer.js  # Canvas ring visualization
│       │       └── event_handler.js    # Event utilities
│       └── templates/
│           ├── index.html              # Main HTML template
│           └── components/             # HTML components
│               ├── control_panel.html
│               ├── ring_view.html
│               ├── node_details.html
│               └── event_log.html
│
├── chord/
│   └── node.py                         # Chord node implementation
│
└── consistency/
    └── quorum.py                       # Quorum consensus logic
```

## ⚙️ Configuration

### Hash Function

The simulation uses a simple modulo-based hash function:

```python
# For numeric keys: hash = int(key) % 256
# For string keys: hash = sum(ord(c) for c in key) % 256
```

### Replication Settings

- **Replication Factor (N):** 3 nodes
- **Write Quorum (W):** 2 nodes (default)
- **Read Quorum (R):** 2 nodes (default)

With R=2, W=2, N=3: **R + W > N** → **Strong Consistency** guaranteed

### Modify Settings

Edit `simulation/simulator.py`:

```python
def __init__(self, m=8):
    self.m = 8                    # Hash space: 2^8 = 256
    self.ring_size = 2 ** m       # Total positions: 256
    self.replication_factor = 3   # Number of replicas
```

## 🔧 Advanced Usage

### Using Shell Scripts

**Fish Shell:**
```fish
chmod +x run_simulation.fish
./run_simulation.fish
```

**Bash:**
```bash
chmod +x run_simulation.sh
./run_simulation.sh
```

### Custom Port

If port 5000 is in use, edit `simulation/visualization/server.py`:

```python
socketio.run(app, debug=True, host='0.0.0.0', port=5001)
```

### API Endpoints

The server exposes REST API endpoints:

- `GET /api/nodes` - Get all nodes
- `GET /api/ring` - Get ring visualization data
- `POST /api/node/join` - Add a node
- `POST /api/node/leave` - Remove a node
- `POST /api/node/offline` - Set node offline
- `POST /api/node/online` - Bring node online
- `POST /api/put` - Store key-value pair
- `GET /api/get?key=<key>` - Retrieve value
- `GET /api/events` - Get event log
- `POST /api/events/clear` - Clear event log

### Example API Usage

```bash
# Add a node
curl -X POST http://localhost:5000/api/node/join \
  -H "Content-Type: application/json" \
  -d '{"node_id": "42", "address": "192.168.1.42"}'

# Store data
curl -X POST http://localhost:5000/api/put \
  -H "Content-Type: application/json" \
  -d '{"key": "username", "value": "alice", "write_quorum": 2}'

# Retrieve data
curl http://localhost:5000/api/get?key=username&read_quorum=2
```

## 🐛 Troubleshooting

### Virtual Environment Issues

If activation fails, try:

```fish
# Delete old venv
rm -rf venv

# Recreate
python3 -m venv venv
source venv/bin/activate.fish
pip install -r requirements.txt
```

### Import Errors

Make sure you're in the project root directory:

```fish
pwd
# Should show: /home/mahendrakerarya/codeforces/Distributed_Sys/Ds_courseproject (1)/chord-dht-simulation
```

### Port Already in Use

Find and kill the process using port 5000:

```fish
# Find process
lsof -i :5000

# Kill it
kill -9 <PID>
```

Or use a different port (see Custom Port section above).

### Canvas Not Displaying

1. Clear browser cache
2. Check browser console for JavaScript errors (F12)
3. Refresh the page
4. Try a different browser

## 📚 Understanding the Simulation

### Chord DHT Basics

- **Consistent Hashing:** Keys and nodes are hashed to a circular space (0-255)
- **Successor:** Each key is stored on its successor node (first node ≥ key hash)
- **Replication:** Data is replicated to N successor nodes for fault tolerance
- **Quorum:** R nodes must agree on reads, W nodes must confirm writes

### Quorum Consistency

With **N=3, R=2, W=2**:

- Any read queries 3 replicas and requires 2 matching responses
- Any write must succeed on at least 2 of 3 replicas
- Since R + W > N, reads always see recent writes (**Strong Consistency**)

### Hinted Handoff

When a replica node is offline:

1. The write is temporarily stored as a "hint" on another node
2. When the offline node comes back online
3. The hints are transferred to it
4. This ensures eventual consistency despite temporary failures

## 🎓 Example Workflows

### Workflow 1: Basic Operations

```
1. Add nodes: 10, 50, 100, 150, 200
2. PUT key="user1", value="Alice"
3. PUT key="user2", value="Bob"
4. Click each node to see data distribution
5. GET key="user1" to retrieve value
```

### Workflow 2: Fault Tolerance

```
1. Add nodes: 0, 64, 128, 192
2. PUT key="data", value="important"
3. Select node 64, click "Set Offline"
4. GET key="data" (should still work due to replication)
5. Select node 64, click "Set Online"
6. Check event log for hinted handoff recovery
```

### Workflow 3: Node Dynamics

```
1. Add nodes: 30, 90, 150, 210
2. PUT multiple keys
3. Add new node: 60
4. Observe key redistribution in event log
5. Remove node: 90
6. See how data moves to remaining nodes
```

## 📝 Dependencies

The simulation requires:

- **Flask** - Web framework
- **Flask-SocketIO** - WebSocket support for real-time updates
- **python-socketio** - SocketIO implementation

All dependencies are listed in `requirements.txt`.

## 🤝 Contributing

This is a course project for Distributed Systems. Feel free to:

- Report bugs
- Suggest features
- Submit improvements

## 📄 License

Educational project for Distributed Systems coursework.

## 🙏 Credits

Implements Chord DHT concepts including:
- Consistent hashing
- Successor-based key location
- Replication and fault tolerance
- Quorum-based consistency
- Hinted handoff mechanism

---

**Need more help?** Check `QUICKSTART.md` for detailed usage instructions.

**Questions?** Check the event log in the web interface - it shows what's happening at each step!