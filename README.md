# Chord DHT with Eventual Consistency & Fault Tolerance

A production-ready implementation of the Chord Distributed Hash Table (DHT) protocol with advanced features including quorum-based replication, sloppy quorum, hinted handoff, and vector clock-based versioning for eventual consistency.

## Table of Contents
- [Features](#features)
- [Architecture](#architecture)
- [Setup Instructions](#setup-instructions)
- [Running the System](#running-the-system)
- [Testing](#testing)
- [API Reference](#api-reference)
- [Implementation Details](#implementation-details)

---

## Features

### Core Chord Protocol
- ✅ **Consistent Hashing**: SHA-1 based key distribution across nodes
- ✅ **Finger Table Routing**: Efficient O(log N) lookup with full ring knowledge
- ✅ **Stabilization Protocol**: Automatic ring maintenance and failure detection
- ✅ **Dynamic Join/Leave**: Nodes can join/leave with automatic key redistribution

### Replication & Consistency
- ✅ **Quorum-based Operations**: Configurable N, R, W parameters (Dynamo-style)
- ✅ **Sloppy Quorum**: High availability when primary replicas are unavailable
- ✅ **Hinted Handoff**: Automatic recovery when failed nodes rejoin
- ✅ **Vector Clocks**: Causality tracking and conflict detection
- ✅ **Read Repair**: Automatic consistency restoration during reads

### Fault Tolerance
- ✅ **Persistent Storage**: Data survives node restarts (human-readable .txt format)
- ✅ **Backup-aware GET**: Reads succeed even when primary nodes are down
- ✅ **Automatic Recovery**: Failed nodes recover latest data when rejoining
- ✅ **Successor List**: Multiple successors for redundancy

### Additional Features
- ✅ **RESTful API**: HTTP interface for easy client interaction
- ✅ **Comprehensive Testing**: Unit tests and integration test suite
- ✅ **Detailed Logging**: Debug and monitor system behavior
- ✅ **Storage Inspection**: Human-readable storage format for debugging

---

## Architecture

### System Components

```
┌─────────────────────────────────────────────────────────────┐
│                     Chord DHT System                         │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   Node 1     │  │   Node 2     │  │   Node 3     │     │
│  │              │  │              │  │              │     │
│  │ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │     │
│  │ │  Finger  │ │  │ │  Finger  │ │  │ │  Finger  │ │     │
│  │ │  Table   │ │  │ │  Table   │ │  │ │  Table   │ │     │
│  │ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │     │
│  │ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │     │
│  │ │ Storage  │ │  │ │ Storage  │ │  │ │ Storage  │ │     │
│  │ │ Primary  │ │  │ │ Primary  │ │  │ │ Primary  │ │     │
│  │ │ Backup   │ │  │ │ Backup   │ │  │ │ Backup   │ │     │
│  │ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│                                                              │
│  Communication: Asyncio UDP + HTTP REST API                 │
└─────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

1. **Full Ring Knowledge**: Each node maintains references to ALL other nodes for optimal replica selection
2. **Quorum System**: N=3, R=2, W=2 (configurable) for strong consistency guarantees
3. **Vector Clocks**: Track causality and detect concurrent updates
4. **Persistent Storage**: Text-based format for easy inspection and debugging
5. **Sloppy Quorum**: Store replicas on next available nodes when primaries fail

### Storage Structure

```
storage/
├── node_13/
│   ├── primary/        # Keys this node is responsible for
│   │   ├── key1.txt
│   │   └── key2.txt
│   └── backup/         # Replicas from other nodes
│       ├── node_35/    # Backups for Node 35
│       │   └── key3.txt
│       └── node_50/    # Backups for Node 50
│           └── key4.txt
```

Each `.txt` file contains:
```json
{
  "key": "mykey",
  "value": "myvalue",
  "version": {"13": 2, "50": 1},
  "type": "primary"
}
```

---

## Setup Instructions

### Prerequisites

- **Python**: 3.8 or higher
- **Operating System**: Linux, macOS, or Windows
- **Network**: All machines must be able to communicate (same network or proper firewall rules)

### Installation

#### 1. Clone or Extract the Project

```bash
cd /path/to/project
cd "Ds_courseproject (1)"
```

#### 2. Create Virtual Environment

**On Linux/macOS:**
```bash
python3 -m venv venv
source venv/bin/activate
```

**On Windows:**
```cmd
python -m venv venv
venv\Scripts\activate
```

**On Fish Shell (Linux/macOS):**
```fish
python3 -m venv venv
source venv/bin/activate.fish
```

#### 3. Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

#### 4. Verify Installation

```bash
python -c "import asyncio; import aiohttp; print('Dependencies OK')"
```

---

## Running the System

### Single Machine Setup (Local Testing)

#### Terminal 1: Start First Node (Bootstrap Node)
```bash
python3 main.py --port 5001 --host 0.0.0.0 --m 6 --N 3 --R 2 --W 2
```

This creates a new Chord ring.

#### Terminal 2: Start Second Node
```bash
python3 main.py --port 5002 --host 0.0.0.0 --m 6 --N 3 --R 2 --W 2 --join 127.0.0.1:5001
```

#### Terminal 3: Start Third Node
```bash
python3 main.py --port 5003 --host 0.0.0.0 --m 6 --N 3 --R 2 --W 2 --join 127.0.0.1:5001
```

#### Terminal 4: Client Operations
```bash
# PUT a key-value pair
curl -X PUT "http://127.0.0.1:8001/put?key=mykey&value=myvalue"

# GET a value
curl "http://127.0.0.1:8001/get?key=mykey"

# View all keys on a node
curl "http://127.0.0.1:8001/keys"

# View ring information
curl "http://127.0.0.1:8001/ring"
```

### Multi-Machine Setup (Distributed Deployment)

#### Prerequisites
- All machines on the same network
- Firewall allows UDP (ports 5000-5999) and HTTP (ports 8000-8999)
- Note the IP addresses of all machines

#### Machine 1 (Bootstrap Node) - IP: 192.168.1.100
```bash
python3 main.py --port 5001 --host 0.0.0.0 --m 6 --N 3 --R 2 --W 2
```

#### Machine 2 - IP: 192.168.1.101
```bash
python3 main.py --port 5001 --host 0.0.0.0 --m 6 --N 3 --R 2 --W 2 --join 192.168.1.100:5001
```

#### Machine 3 - IP: 192.168.1.102
```bash
python3 main.py --port 5001 --host 0.0.0.0 --m 6 --N 3 --R 2 --W 2 --join 192.168.1.100:5001
```

#### Client (Any Machine)
```bash
# Access any node in the cluster
curl -X PUT "http://192.168.1.100:8001/put?key=test&value=hello"
curl "http://192.168.1.101:8001/get?key=test"
```

### Command-Line Parameters

| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `--port` | UDP port for Chord protocol | 5000 | `--port 5001` |
| `--host` | Host to bind to | 0.0.0.0 | `--host 0.0.0.0` |
| `--m` | Bit size of identifier space (2^m nodes max) | 6 | `--m 8` |
| `--N` | Number of replicas | 3 | `--N 5` |
| `--R` | Read quorum size | 2 | `--R 3` |
| `--W` | Write quorum size | 2 | `--W 3` |
| `--join` | Address of existing node to join | None | `--join 192.168.1.100:5001` |
| `--log-level` | Logging level | INFO | `--log-level DEBUG` |

### Configuration Examples

#### High Availability (R=1, W=1)
```bash
python3 main.py --port 5001 --N 3 --R 1 --W 1
```
- Fastest performance
- Lower consistency guarantee
- Can tolerate N-1 failures

#### Strong Consistency (R=3, W=3)
```bash
python3 main.py --port 5001 --N 3 --R 3 --W 3
```
- Strongest consistency
- Slower performance
- Cannot tolerate any failures

#### Balanced (R=2, W=2) - Recommended
```bash
python3 main.py --port 5001 --N 3 --R 2 --W 2
```
- Good consistency
- Good availability
- Can tolerate 1 failure

---

## Testing

### Run Comprehensive Test Suite

```bash
python3 test_comprehensive.py
```

This runs multiple test scenarios:
- ✅ Basic PUT/GET operations
- ✅ Multi-node replication
- ✅ Node failure and recovery
- ✅ Sloppy quorum with hinted handoff
- ✅ Vector clock versioning
- ✅ Concurrent updates
- ✅ Read repair

### Manual Testing Scenarios

#### Scenario 1: Basic Operations
```bash
# Start 3 nodes (in separate terminals)
python3 main.py --port 5001 --N 3 --R 2 --W 2
python3 main.py --port 5002 --N 3 --R 2 --W 2 --join 127.0.0.1:5001
python3 main.py --port 5003 --N 3 --R 2 --W 2 --join 127.0.0.1:5001

# Test operations
curl -X PUT "http://127.0.0.1:8001/put?key=test1&value=hello"
curl "http://127.0.0.1:8001/get?key=test1"
# Expected: {"value": "hello"}
```

#### Scenario 2: Node Failure Recovery
```bash
# Start 3 nodes
# ... (same as above)

# PUT a key
curl -X PUT "http://127.0.0.1:8001/put?key=abc&value=100"

# Kill one node (Ctrl+C on terminal 2)

# PUT updated value (sloppy quorum)
curl -X PUT "http://127.0.0.1:8001/put?key=abc&value=200"

# Restart the killed node
python3 main.py --port 5002 --N 3 --R 2 --W 2 --join 127.0.0.1:5001

# Wait 5 seconds for recovery

# Verify recovered value
curl "http://127.0.0.1:8002/get?key=abc"
# Expected: {"value": "200"}
```

#### Scenario 3: Persistence Test
```bash
# Start node
python3 main.py --port 5001 --N 3 --R 2 --W 2

# PUT data
curl -X PUT "http://127.0.0.1:8001/put?key=persistent&value=data123"

# Kill node (Ctrl+C)

# Restart node
python3 main.py --port 5001 --N 3 --R 2 --W 2

# Verify data persisted
curl "http://127.0.0.1:8001/get?key=persistent"
# Expected: {"value": "data123"}

# Check storage file
cat storage/node_*/primary/persistent.txt
```

### Inspect Storage

```bash
# View all storage directories
ls -R storage/

# View a specific key file
cat storage/node_13/primary/mykey.txt

# View backups for a specific node
ls storage/node_50/backup/node_35/
```

---

## API Reference

### HTTP REST API

All nodes expose an HTTP API on port `8000 + (chord_port - 5000)`.
Example: Chord port 5001 → HTTP port 8001

#### PUT - Store a key-value pair
```http
PUT /put?key=<key>&value=<value>
```

**Example:**
```bash
curl -X PUT "http://127.0.0.1:8001/put?key=username&value=john"
```

**Response:**
```json
{
  "status": "ok"
}
```

#### GET - Retrieve a value
```http
GET /get?key=<key>
```

**Example:**
```bash
curl "http://127.0.0.1:8001/get?key=username"
```

**Response:**
```json
{
  "value": "john"
}
```

#### GET /keys - List all keys on this node
```http
GET /keys
```

**Response:**
```json
{
  "primary_keys": ["key1", "key2"],
  "backup_keys": {
    "node_35": ["key3"],
    "node_50": ["key4"]
  }
}
```

#### GET /ring - View ring topology
```http
GET /ring
```

**Response:**
```json
{
  "node_id": 13,
  "address": "192.168.1.100:5001",
  "predecessor": {"node_id": 50, "address": "192.168.1.101:5001"},
  "successor": {"node_id": 35, "address": "192.168.1.102:5001"},
  "all_nodes": [
    {"node_id": 13, "address": "192.168.1.100:5001"},
    {"node_id": 35, "address": "192.168.1.102:5001"},
    {"node_id": 50, "address": "192.168.1.101:5001"}
  ]
}
```

---

## Implementation Details

### Quorum-Based Replication

The system uses **Dynamo-style quorum replication**:

- **N**: Total number of replicas (default: 3)
- **R**: Read quorum - minimum replicas to read from (default: 2)
- **W**: Write quorum - minimum replicas to acknowledge write (default: 2)

**Consistency Levels:**
- **R + W > N**: Strong consistency (overlapping quorum)
- **R + W ≤ N**: Eventual consistency

### Sloppy Quorum & Hinted Handoff

When a primary replica is unavailable:

1. **Sloppy Quorum**: Store on next available node
2. **Hinted Handoff**: Mark replica with original owner's ID
3. **Automatic Recovery**: When failed node returns, transfer hints back

**Example:**
```
Normal: Key X → Node 35 (primary), Node 50 (backup), Node 62 (backup)
Node 35 Down: Key X → Node 50 (hint for 35), Node 62 (hint for 35), Node 13 (hint for 35)
Node 35 Returns: Node 50, 62, 13 send hints back → Node 35 (primary)
```

### Vector Clocks

Each value has a **vector clock** tracking causality:

```python
version = {13: 2, 50: 1, 62: 1}
```

- **Key**: Node ID
- **Value**: Logical timestamp

**Version Comparison:**
- `v1 < v2`: v1 happens before v2 (v2 is newer)
- `v1 || v2`: Concurrent (need conflict resolution)

**On PUT:**
- Check existing version
- Increment from existing: `{13: 1}` → `{13: 2}`
- Never reset to `{13: 1}` again

**On Recovery:**
- Compare versions
- If concurrent (node was down), accept recovered version and merge clocks

### Stabilization Protocol

Nodes periodically (every 5 seconds):
1. **Stabilize**: Check successor's predecessor, update if needed
2. **Fix Fingers**: Refresh finger table entries
3. **Update Successor List**: Query successors for their successors
4. **Check Predecessor**: Ping predecessor to detect failures

### Fault Detection

- **Timeout**: 5 seconds for network requests
- **Retry**: No automatic retry (client responsibility)
- **Failure Handling**: Use sloppy quorum if primary unavailable

---

## Troubleshooting

### Issue: "Connection refused"
**Cause**: Node not started or firewall blocking
**Solution:**
```bash
# Check if node is running
netstat -tulpn | grep 5001

# On Linux, allow UDP ports
sudo ufw allow 5000:5999/udp
sudo ufw allow 8000:8999/tcp
```

### Issue: "Quorum write failed"
**Cause**: Not enough replicas available
**Solution:**
- Ensure N nodes are running (N = replica count)
- Check network connectivity between nodes
- Lower W value for more availability

### Issue: "Key not found"
**Cause**: Key was stored on failed nodes
**Solution:**
- Restart failed nodes to trigger recovery
- Check backup storage: `ls storage/*/backup/*/`

### Issue: Virtual environment activation fails
**Bash/Zsh:**
```bash
source venv/bin/activate
```

**Fish:**
```fish
source venv/bin/activate.fish
```

**Windows CMD:**
```cmd
venv\Scripts\activate.bat
```

**Windows PowerShell:**
```powershell
venv\Scripts\Activate.ps1
```

---

## Performance Characteristics

- **Lookup**: O(log N) hops (with finger table) or O(1) (with full ring knowledge)
- **Join**: O(N) - broadcasts to all nodes
- **Storage**: O(K * R) where K = keys, R = replicas
- **Network**: UDP for Chord protocol, HTTP for client API
- **Scalability**: Tested up to 64 nodes (m=6)

---

## Project Structure

```
Ds_courseproject (1)/
├── chord/
│   ├── __init__.py
│   ├── node.py              # Core Chord node implementation
│   ├── routing.py           # Finger table and routing logic
│   └── storage.py           # Persistent key-value storage
├── communication/
│   ├── __init__.py
│   ├── message.py           # Message protocol definitions
│   └── network.py           # UDP network manager
├── consistency/
│   ├── __init__.py
│   ├── quorum.py            # Quorum-based read/write
│   ├── replication.py       # Replication manager
│   └── vector_clock.py      # Vector clock implementation
├── http_api/
│   ├── __init__.py
│   └── server.py            # HTTP REST API server
├── storage/                 # Persistent storage (generated)
├── main.py                  # Entry point
├── config.py                # Configuration
├── test_comprehensive.py    # Test suite
├── requirements.txt         # Python dependencies
└── README.md               # This file
```

---

## Contributors

Distributed Systems Course Project

---

## License

Educational use only - Distributed Systems course project.

---

## Quick Start Checklist

- [ ] Python 3.8+ installed
- [ ] Created virtual environment: `python3 -m venv venv`
- [ ] Activated venv: `source venv/bin/activate` (Linux/Mac) or `venv\Scripts\activate` (Windows)
- [ ] Installed dependencies: `pip install -r requirements.txt`
- [ ] Started first node: `python3 main.py --port 5001`
- [ ] Started additional nodes with `--join` parameter
- [ ] Tested PUT: `curl -X PUT "http://127.0.0.1:8001/put?key=test&value=hello"`
- [ ] Tested GET: `curl "http://127.0.0.1:8001/get?key=test"`
- [ ] Ran test suite: `python3 test_comprehensive.py`

**You're ready to go!** 🚀
