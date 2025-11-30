"""
Configuration parameters for the Chord DHT system with consistency.
"""

# Chord Parameters
M = 6  # Identifier space size: 2^M positions (default: 64 nodes)
IDENTIFIER_SPACE = 2 ** M

# Replication Parameters
N = 3  # Number of replicas
R = 2  # Read quorum size
W = 2  # Write quorum size

# Network Parameters
DEFAULT_PORT = 5000
SOCKET_TIMEOUT = 5.0  # seconds
MAX_MESSAGE_SIZE = 4096  # bytes
HEARTBEAT_INTERVAL = 5.0  # seconds

# Stabilization Parameters
STABILIZE_INTERVAL = 3.0  # seconds
FIX_FINGERS_INTERVAL = 3.0  # seconds
CHECK_PREDECESSOR_INTERVAL = 3.0  # seconds

# Replication and Consistency
REPLICATION_TIMEOUT = 2.0  # seconds
READ_REPAIR_ENABLED = True

# Logging
LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Testing
TEST_MODE = False
SIMULATE_FAILURES = False
FAILURE_PROBABILITY = 0.1  # 10% failure rate in test mode

def validate_quorum():
    """Validate quorum configuration."""
    if R + W > N:
        consistency = "STRONG (R+W > N)"
    elif R + W == N:
        consistency = "MODERATE (R+W = N)"
    else:
        consistency = "EVENTUAL (R+W < N)"
    
    return {
        "N": N,
        "R": R,
        "W": W,
        "consistency_level": consistency,
        "valid": R <= N and W <= N and R > 0 and W > 0
    }

if __name__ == "__main__":
    # Validate and print configuration
    config = validate_quorum()
    print("Chord Configuration:")
    print(f"  Identifier Space: 2^{M} = {IDENTIFIER_SPACE}")
    print(f"  Replicas (N): {config['N']}")
    print(f"  Read Quorum (R): {config['R']}")
    print(f"  Write Quorum (W): {config['W']}")
    print(f"  Consistency Level: {config['consistency_level']}")
    print(f"  Valid: {config['valid']}")
