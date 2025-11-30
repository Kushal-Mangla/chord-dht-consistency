// Main application logic for Chord DHT Simulation

// Global state
let nodes = {};
let selectedNodeId = null;
const socket = io();

// Initialize on page load
document.addEventListener("DOMContentLoaded", function() {
    initializeEventHandlers();
    initializeWebSocket();
    loadInitialData();
});

function initializeEventHandlers() {
    // Node actions
    document.getElementById("join-node-btn")?.addEventListener("click", handleJoinNode);
    document.getElementById("offline-node-btn")?.addEventListener("click", handleOfflineNode);
    document.getElementById("online-node-btn")?.addEventListener("click", handleOnlineNode);
    
    // Key-value actions
    document.getElementById("put-btn")?.addEventListener("click", handlePut);
    document.getElementById("get-btn")?.addEventListener("click", handleGet);
    
    // Event log actions
    document.getElementById("clear-log-btn")?.addEventListener("click", handleClearLog);
}

function initializeWebSocket() {
    socket.on('connect', function() {
        console.log('Connected to server');
        addLogEntry('Connected to simulation server', 'info');
    });
    
    socket.on('node_update', function(data) {
        nodes = data.nodes;
        updateNodesList();
        updateRingVisualization();
        if (selectedNodeId && nodes[selectedNodeId]) {
            updateNodeDetails(selectedNodeId);
        }
    });
    
    socket.on('log_update', function(data) {
        updateEventLog(data.log);
    });
}

async function loadInitialData() {
    try {
        const response = await fetch('/api/nodes');
        const data = await response.json();
        if (data.status === 'success') {
            nodes = data.nodes;
            updateNodesList();
            updateRingVisualization();
        }
        
        const eventsResponse = await fetch('/api/events?limit=20');
        const eventsData = await eventsResponse.json();
        if (eventsData.status === 'success') {
            updateEventLog(eventsData.events);
        }
    } catch (error) {
        console.error('Error loading initial data:', error);
        addLogEntry('Error loading initial data', 'error');
    }
}

async function handleJoinNode() {
    const nodeId = document.getElementById("node-id-input").value;
    if (!nodeId) {
        alert('Please enter a node ID');
        return;
    }
    
    try {
        const response = await fetch('/api/node/join', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ node_id: nodeId })
        });
        
        const data = await response.json();
        if (data.status === 'success') {
            document.getElementById("node-id-input").value = '';
            addLogEntry(data.message, 'success');
        } else {
            addLogEntry(data.message, 'error');
        }
    } catch (error) {
        console.error('Error joining node:', error);
        addLogEntry('Error joining node', 'error');
    }
}

// Removed handleLeaveNode function - node removal feature disabled

async function handleOfflineNode() {
    if (!selectedNodeId) {
        alert('Please select a node first');
        return;
    }
    
    try {
        const response = await fetch('/api/node/offline', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ node_id: selectedNodeId })
        });
        
        const data = await response.json();
        addLogEntry(data.message, data.status);
    } catch (error) {
        console.error('Error setting node offline:', error);
        addLogEntry('Error setting node offline', 'error');
    }
}

async function handleOnlineNode() {
    if (!selectedNodeId) {
        alert('Please select a node first');
        return;
    }
    
    try {
        const response = await fetch('/api/node/online', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ node_id: selectedNodeId })
        });
        
        const data = await response.json();
        addLogEntry(data.message, data.status);
    } catch (error) {
        console.error('Error bringing node online:', error);
        addLogEntry('Error bringing node online', 'error');
    }
}

async function handlePut() {
    const key = document.getElementById("key-input").value;
    const value = document.getElementById("value-input").value;
    
    if (!key || !value) {
        alert('Please enter both key and value');
        return;
    }
    
    try {
        const response = await fetch('/api/put', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ key, value })
        });
        
        const data = await response.json();
        if (data.status === 'success') {
            document.getElementById("key-input").value = '';
            document.getElementById("value-input").value = '';
            addLogEntry(data.message, 'success');
        } else {
            addLogEntry(data.message, 'error');
        }
    } catch (error) {
        console.error('Error storing key-value:', error);
        addLogEntry('Error storing key-value', 'error');
    }
}

async function handleGet() {
    const key = document.getElementById("key-input").value;
    
    if (!key) {
        alert('Please enter a key');
        return;
    }
    
    try {
        const response = await fetch(`/api/get?key=${encodeURIComponent(key)}`);
        const data = await response.json();
        
        if (data.status === 'success') {
            document.getElementById("value-input").value = data.value;
            addLogEntry(`Retrieved: ${key} = ${data.value}`, 'success');
        } else {
            addLogEntry(data.message, 'error');
        }
    } catch (error) {
        console.error('Error retrieving value:', error);
        addLogEntry('Error retrieving value', 'error');
    }
}

async function handleClearLog() {
    try {
        await fetch('/api/events/clear', { method: 'POST' });
        document.getElementById('event-log-list').innerHTML = '';
    } catch (error) {
        console.error('Error clearing log:', error);
    }
}

function updateNodesList() {
    const nodesList = document.getElementById('nodes-list');
    if (!nodesList) return;
    
    nodesList.innerHTML = '';
    
    const sortedNodeIds = Object.keys(nodes).sort((a, b) => parseInt(a) - parseInt(b));
    
    sortedNodeIds.forEach(nodeId => {
        const node = nodes[nodeId];
        const nodeItem = document.createElement('div');
        nodeItem.className = `node-item ${node.state}`;
        if (String(nodeId) === String(selectedNodeId)) {
            nodeItem.classList.add('selected');
        }
        
        nodeItem.innerHTML = `
            <div class="node-id">Node ${nodeId}</div>
            <div class="node-status">${node.state}</div>
            <div class="node-keys">${node.key_count} keys</div>
        `;
        
        // Fix: Use closure to capture the correct node ID
        nodeItem.addEventListener('click', ((currentNodeId) => {
            return function() {
                selectedNodeId = currentNodeId;
                console.log('Selected node:', selectedNodeId); // Debug log
                updateNodesList();
                updateNodeDetails(currentNodeId);
            };
        })(nodeId));
        
        nodesList.appendChild(nodeItem);
    });
}

function updateNodeDetails(nodeId) {
    const node = nodes[nodeId];
    if (!node) return;
    
    const detailsDiv = document.getElementById('node-details-content');
    if (!detailsDiv) return;
    
    let storageHtml = '<div class="storage-empty">No keys stored</div>';
    if (Object.keys(node.storage).length > 0) {
        storageHtml = '<table class="storage-table"><thead><tr><th>Key</th><th>Value</th></tr></thead><tbody>';
        for (const [key, value] of Object.entries(node.storage)) {
            storageHtml += `<tr><td>${key}</td><td>${value}</td></tr>`;
        }
        storageHtml += '</tbody></table>';
    }
    
    detailsDiv.innerHTML = `
        <h3>Node ${nodeId}</h3>
        <div class="detail-row">
            <span class="detail-label">Address:</span>
            <span class="detail-value">${node.address}</span>
        </div>
        <div class="detail-row">
            <span class="detail-label">Status:</span>
            <span class="detail-value status-${node.state}">${node.state}</span>
        </div>
        <div class="detail-row">
            <span class="detail-label">Successor:</span>
            <span class="detail-value">${node.successor || 'None'}</span>
        </div>
        <div class="detail-row">
            <span class="detail-label">Replicas:</span>
            <span class="detail-value">${node.replicas.join(', ') || 'None'}</span>
        </div>
        <div class="detail-section">
            <h4>Storage (${node.key_count} keys)</h4>
            ${storageHtml}
        </div>
    `;
}

function updateEventLog(events) {
    const logList = document.getElementById('event-log-list');
    if (!logList) return;
    
    logList.innerHTML = '';
    
    events.slice().reverse().forEach(event => {
        const logItem = document.createElement('div');
        logItem.className = `log-item log-${event.type}`;
        
        const timestamp = new Date(event.timestamp * 1000).toLocaleTimeString();
        logItem.innerHTML = `
            <span class="log-time">${timestamp}</span>
            <span class="log-type">${event.type}</span>
            <span class="log-message">${event.message}</span>
        `;
        
        logList.appendChild(logItem);
    });
}

function addLogEntry(message, type) {
    const logList = document.getElementById('event-log-list');
    if (!logList) return;
    
    const logItem = document.createElement('div');
    logItem.className = `log-item log-${type}`;
    
    const timestamp = new Date().toLocaleTimeString();
    logItem.innerHTML = `
        <span class="log-time">${timestamp}</span>
        <span class="log-type">${type}</span>
        <span class="log-message">${message}</span>
    `;
    
    logList.insertBefore(logItem, logList.firstChild);
}