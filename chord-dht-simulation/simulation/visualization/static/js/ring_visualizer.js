// Ring visualization for Chord DHT

let canvas, ctx;
let ringRadius = 200;
let centerX, centerY;

function initializeRingVisualization() {
    canvas = document.getElementById('ring-canvas');
    if (!canvas) return;
    
    ctx = canvas.getContext('2d');
    
    // Set canvas size
    canvas.width = canvas.offsetWidth;
    canvas.height = canvas.offsetHeight;
    
    centerX = canvas.width / 2;
    centerY = canvas.height / 2;
    ringRadius = Math.min(centerX, centerY) - 60;
}

function updateRingVisualization() {
    if (!canvas || !ctx) {
        initializeRingVisualization();
    }
    
    if (!canvas || !ctx) return;
    
    // Clear canvas
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    
    // Draw ring
    drawRing();
    
    // Draw nodes
    const nodeIds = Object.keys(nodes).sort((a, b) => parseInt(a) - parseInt(b));
    
    if (nodeIds.length === 0) {
        drawEmptyMessage();
        return;
    }
    
    const ringSize = 256; // 2^8
    
    // Draw successor connections
    ctx.strokeStyle = 'rgba(52, 152, 219, 0.3)';
    ctx.lineWidth = 1;
    nodeIds.forEach((nodeId, index) => {
        const node = nodes[nodeId];
        if (node.successor) {
            const pos1 = getNodePosition(parseInt(nodeId), ringSize);
            const pos2 = getNodePosition(parseInt(node.successor), ringSize);
            
            ctx.beginPath();
            ctx.moveTo(pos1.x, pos1.y);
            ctx.lineTo(pos2.x, pos2.y);
            ctx.stroke();
        }
    });
    
    // Draw nodes
    nodeIds.forEach((nodeId, index) => {
        const node = nodes[nodeId];
        const pos = getNodePosition(parseInt(nodeId), ringSize);
        
        drawNode(pos.x, pos.y, nodeId, node);
    });
    
    // Draw hash values on ring
    drawHashMarkers(ringSize);
}

function drawRing() {
    ctx.strokeStyle = '#34495e';
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.arc(centerX, centerY, ringRadius, 0, 2 * Math.PI);
    ctx.stroke();
}

function drawNode(x, y, nodeId, node) {
    const isSelected = nodeId === selectedNodeId;
    const isOnline = node.state === 'online';
    
    // Node circle
    ctx.beginPath();
    ctx.arc(x, y, isSelected ? 18 : 15, 0, 2 * Math.PI);
    
    // Fill color based on state
    if (isOnline) {
        ctx.fillStyle = isSelected ? '#27ae60' : '#2ecc71';
    } else {
        ctx.fillStyle = isSelected ? '#c0392b' : '#e74c3c';
    }
    ctx.fill();
    
    // Border
    ctx.strokeStyle = isSelected ? '#f39c12' : '#34495e';
    ctx.lineWidth = isSelected ? 3 : 1;
    ctx.stroke();
    
    // Node ID text
    ctx.fillStyle = 'white';
    ctx.font = 'bold 12px Arial';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(nodeId, x, y);
    
    // Key count badge
    if (node.key_count > 0) {
        const badgeX = x + 12;
        const badgeY = y - 12;
        
        ctx.beginPath();
        ctx.arc(badgeX, badgeY, 10, 0, 2 * Math.PI);
        ctx.fillStyle = '#e67e22';
        ctx.fill();
        ctx.strokeStyle = 'white';
        ctx.lineWidth = 2;
        ctx.stroke();
        
        ctx.fillStyle = 'white';
        ctx.font = 'bold 10px Arial';
        ctx.fillText(node.key_count, badgeX, badgeY);
    }
}

function drawHashMarkers(ringSize) {
    ctx.fillStyle = '#7f8c8d';
    ctx.font = '10px Arial';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    
    const markers = [0, 64, 128, 192];
    markers.forEach(hash => {
        const angle = (hash / ringSize) * 2 * Math.PI - Math.PI / 2;
        const outerX = centerX + Math.cos(angle) * (ringRadius + 25);
        const outerY = centerY + Math.sin(angle) * (ringRadius + 25);
        
        ctx.fillText(hash.toString(), outerX, outerY);
    });
}

function drawEmptyMessage() {
    ctx.fillStyle = '#95a5a6';
    ctx.font = '16px Arial';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText('No nodes in the ring', centerX, centerY);
    ctx.font = '12px Arial';
    ctx.fillText('Add a node to get started', centerX, centerY + 25);
}

function getNodePosition(nodeId, ringSize) {
    const angle = (nodeId / ringSize) * 2 * Math.PI - Math.PI / 2; // Start from top
    const x = centerX + Math.cos(angle) * ringRadius;
    const y = centerY + Math.sin(angle) * ringRadius;
    return { x, y };
}

// Handle canvas resize
window.addEventListener('resize', function() {
    if (canvas) {
        canvas.width = canvas.offsetWidth;
        canvas.height = canvas.offsetHeight;
        centerX = canvas.width / 2;
        centerY = canvas.height / 2;
        ringRadius = Math.min(centerX, centerY) - 60;
        updateRingVisualization();
    }
});

// Initialize on load
document.addEventListener("DOMContentLoaded", function() {
    setTimeout(initializeRingVisualization, 100);
});

function connectNodes(id1, id2) {
    connections.push([id1, id2]);
    drawRing();
}

// Event listeners for user actions
document.getElementById('addNodeButton').addEventListener('click', () => {
    const nodeId = document.getElementById('nodeIdInput').value;
    addNode(nodeId);
});

document.getElementById('toggleNodeButton').addEventListener('click', () => {
    const nodeId = document.getElementById('toggleNodeIdInput').value;
    toggleNode(nodeId);
});

document.getElementById('connectNodesButton').addEventListener('click', () => {
    const nodeId1 = document.getElementById('connectNodeId1Input').value;
    const nodeId2 = document.getElementById('connectNodeId2Input').value;
    connectNodes(nodeId1, nodeId2);
});

// Initial draw
drawRing();