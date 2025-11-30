#!/bin/bash

# Chord DHT Simulation Runner Script

echo "==================================="
echo "Chord DHT Simulation Setup"
echo "==================================="

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

echo ""
echo "==================================="
echo "Starting Chord DHT Simulation"
echo "==================================="
echo ""
echo "The simulation will be available at:"
echo "http://localhost:5000"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# Run the server
cd simulation/visualization
python server.py
