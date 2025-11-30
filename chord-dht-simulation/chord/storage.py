class Storage:
    def __init__(self):
        self.data = {}  # Dictionary to store key-value pairs
        self.replicas = {}  # Dictionary to store replicas for each key

    def put(self, key, value, replicas_count=3):
        """Store a key-value pair with specified number of replicas."""
        self.data[key] = value
        self.replicas[key] = []

        # Create replicas
        for i in range(replicas_count):
            replica_key = f"{key}_replica_{i}"
            self.replicas[key].append(replica_key)
            self.data[replica_key] = value  # Store the same value for replicas

    def get(self, key):
        """Retrieve the value for a given key."""
        return self.data.get(key)

    def get_replicas(self, key):
        """Retrieve the replicas for a given key."""
        return self.replicas.get(key, [])

    def remove(self, key):
        """Remove a key and its replicas from storage."""
        if key in self.data:
            del self.data[key]
        if key in self.replicas:
            for replica in self.replicas[key]:
                if replica in self.data:
                    del self.data[replica]
            del self.replicas[key]

    def list_keys(self):
        """List all keys in storage."""
        return list(self.data.keys())