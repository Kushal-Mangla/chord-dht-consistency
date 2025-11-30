class EventManager:
    def __init__(self, simulator):
        self.simulator = simulator
        self.event_log = []

    def log_event(self, event):
        self.event_log.append(event)
        self.update_visualization()

    def update_visualization(self):
        # Logic to update the visualization based on the current state of the simulation
        pass

    def handle_node_join(self, node_id):
        self.log_event(f"Node {node_id} joined the simulation.")
        self.simulator.add_node(node_id)

    def handle_node_leave(self, node_id):
        self.log_event(f"Node {node_id} left the simulation.")
        self.simulator.remove_node(node_id)

    def handle_put_request(self, node_id, key, value):
        self.log_event(f"Node {node_id} put key '{key}' with value '{value}'.")
        self.simulator.store_key_value(node_id, key, value)

    def handle_get_request(self, node_id, key):
        value = self.simulator.get_value(node_id, key)
        self.log_event(f"Node {node_id} requested key '{key}' and received value '{value}'.")
        return value

    def handle_node_failure(self, node_id):
        self.log_event(f"Node {node_id} has failed.")
        self.simulator.mark_node_offline(node_id)

    def handle_node_recovery(self, node_id):
        self.log_event(f"Node {node_id} has recovered.")
        self.simulator.mark_node_online(node_id)