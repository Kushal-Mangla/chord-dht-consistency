class NetworkManager:
    def __init__(self):
        self.nodes = {}

    def add_node(self, node_id, address):
        self.nodes[node_id] = address

    def remove_node(self, node_id):
        if node_id in self.nodes:
            del self.nodes[node_id]

    def send_message(self, sender_id, receiver_id, message):
        if receiver_id in self.nodes:
            # Simulate sending a message to the receiver
            print(f"Message from {sender_id} to {receiver_id}: {message}")
            return True
        return False

    def receive_message(self, node_id, message):
        print(f"Node {node_id} received message: {message}")

    def get_nodes(self):
        return self.nodes.keys()