class Routing:
    def __init__(self, node_id, node_address):
        self.node_id = node_id
        self.node_address = node_address
        self.successor = None
        self.predecessor = None
        self.finger_table = []

    def find_successor(self, id):
        if self.successor and self.successor.node_id >= id:
            return self.successor
        else:
            # Forward the request to the closest preceding node
            return self.closest_preceding_node(id).find_successor(id)

    def closest_preceding_node(self, id):
        for node in reversed(self.finger_table):
            if node.node_id < id:
                return node
        return self  # If no node is found, return self

    def update_finger_table(self, node, index):
        self.finger_table[index] = node

    def notify(self, node):
        if not self.predecessor or node.node_id > self.predecessor.node_id:
            self.predecessor = node

    def get_ring_info(self):
        return {
            'node_id': self.node_id,
            'address': self.node_address,
            'successor': self.successor.node_id if self.successor else None,
            'predecessor': self.predecessor.node_id if self.predecessor else None,
            'finger_table': [n.node_id for n in self.finger_table]
        }