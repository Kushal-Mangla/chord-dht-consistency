class Node:
    def __init__(self, node_id, address):
        self.node_id = node_id
        self.address = address
        self.key_value_store = {}
        self.replicas = {}
        self.is_online = True

    def put(self, key, value):
        if self.is_online:
            self.key_value_store[key] = value
            self.replicate(key, value)
            return True
        return False

    def get(self, key):
        if self.is_online:
            return self.key_value_store.get(key)
        return None

    def replicate(self, key, value):
        # Logic to store replicas on other nodes
        pass

    def go_offline(self):
        self.is_online = False

    def come_online(self):
        self.is_online = True
        # Logic to recover state if necessary
        pass

    def __repr__(self):
        return f"Node({self.node_id}, {self.address}, online={self.is_online})"