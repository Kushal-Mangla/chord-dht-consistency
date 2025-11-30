# Configuration settings for the Chord DHT simulation project

class Config:
    # General settings
    NODE_COUNT = 10  # Default number of nodes in the simulation
    INITIAL_KEY_COUNT = 5  # Default number of keys to pre-populate
    MAX_NODES = 100  # Maximum number of nodes allowed in the simulation
    MAX_KEYS = 1000  # Maximum number of keys allowed in the simulation

    # Network settings
    NETWORK_TIMEOUT = 2.0  # Timeout for network operations in seconds
    REPLICATION_FACTOR = 3  # Number of replicas for each key

    # Visualization settings
    VISUALIZATION_UPDATE_INTERVAL = 1000  # Interval for updating the visualization in milliseconds

    # Event settings
    EVENT_LOG_SIZE = 50  # Maximum number of events to keep in the log

    @staticmethod
    def get_node_address(node_id):
        """Generate a node address based on its ID."""
        return f"localhost:{5000 + node_id}"  # Example address format

    @staticmethod
    def get_key_value_pair():
        """Generate a random key-value pair for simulation."""
        import random
        import string
        key = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        value = ''.join(random.choices(string.ascii_letters + string.digits, k=12))
        return key, value