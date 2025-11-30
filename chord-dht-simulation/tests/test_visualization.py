import unittest
from simulation.visualization.server import app

class TestVisualization(unittest.TestCase):

    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    def test_index_page(self):
        response = self.app.get('/')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Chord DHT Simulation', response.data)

    def test_control_panel_component(self):
        response = self.app.get('/components/control_panel')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Join Node', response.data)

    def test_ring_view_component(self):
        response = self.app.get('/components/ring_view')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Ring View', response.data)

    def test_node_details_component(self):
        response = self.app.get('/components/node_details')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Node Details', response.data)

    def test_event_log_component(self):
        response = self.app.get('/components/event_log')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Event Log', response.data)

if __name__ == '__main__':
    unittest.main()