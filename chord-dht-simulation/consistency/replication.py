class ReplicationManager:
    def __init__(self, node_manager):
        self.node_manager = node_manager

    def replicate(self, key, value, primary_node):
        replicas = self.node_manager.get_replicas(primary_node)
        for replica in replicas:
            self.node_manager.store_value(replica, key, value)

    def get_replicas(self, primary_node):
        # Logic to determine which nodes are replicas of the primary node
        return self.node_manager.get_successors(primary_node)

    def handle_node_failure(self, failed_node):
        # Logic to handle a node going offline
        for node in self.node_manager.nodes:
            if failed_node in self.node_manager.get_replicas(node):
                self.node_manager.remove_replica(node, failed_node)

    def recover_node(self, recovered_node):
        # Logic to recover a node and restore its data
        for node in self.node_manager.nodes:
            if recovered_node in self.node_manager.get_replicas(node):
                self.node_manager.restore_replica(node, recovered_node)