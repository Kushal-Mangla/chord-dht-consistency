import unittest
from simulation.simulator import Simulator
from simulation.node_manager import NodeManager

class TestSimulation(unittest.TestCase):

    def setUp(self):
        self.simulator = Simulator()
        self.node_manager = NodeManager()

    def test_join_node(self):
        initial_node_count = len(self.simulator.nodes)
        self.simulator.join_node("127.0.0.1:5001")
        self.assertEqual(len(self.simulator.nodes), initial_node_count + 1)

    def test_store_key_value(self):
        self.simulator.join_node("127.0.0.1:5001")
        result = self.simulator.store_key_value("key1", "value1")
        self.assertTrue(result)
        self.assertEqual(self.simulator.get_value("key1"), "value1")

    def test_node_offline(self):
        self.simulator.join_node("127.0.0.1:5001")
        self.simulator.join_node("127.0.0.1:5002")
        self.simulator.set_node_offline("127.0.0.1:5001")
        self.assertFalse(self.simulator.is_node_online("127.0.0.1:5001"))

    def test_node_recovery(self):
        self.simulator.join_node("127.0.0.1:5001")
        self.simulator.set_node_offline("127.0.0.1:5001")
        self.simulator.set_node_online("127.0.0.1:5001")
        self.assertTrue(self.simulator.is_node_online("127.0.0.1:5001"))

    def test_replication_on_store(self):
        self.simulator.join_node("127.0.0.1:5001")
        self.simulator.join_node("127.0.0.1:5002")
        self.simulator.store_key_value("key2", "value2")
        replicas = self.node_manager.get_replicas("key2")
        self.assertGreaterEqual(len(replicas), 2)  # Assuming replication factor is set to 2

if __name__ == '__main__':
    unittest.main()