class VectorClock:
    def __init__(self, node_id):
        self.node_id = node_id
        self.clock = {}

    def increment(self):
        if self.node_id in self.clock:
            self.clock[self.node_id] += 1
        else:
            self.clock[self.node_id] = 1

    def update(self, other_clock):
        for node, timestamp in other_clock.items():
            if node in self.clock:
                self.clock[node] = max(self.clock[node], timestamp)
            else:
                self.clock[node] = timestamp

    def merge(self, other_clock):
        merged_clock = VectorClock(self.node_id)
        merged_clock.update(self.clock)
        merged_clock.update(other_clock.clock)
        return merged_clock

    def __lt__(self, other):
        return self.compare(other) == -1

    def __eq__(self, other):
        return self.clock == other.clock

    def compare(self, other):
        self_keys = set(self.clock.keys())
        other_keys = set(other.clock.keys())
        all_keys = self_keys.union(other_keys)

        self_less = False
        other_less = False

        for key in all_keys:
            self_value = self.clock.get(key, 0)
            other_value = other.clock.get(key, 0)

            if self_value < other_value:
                self_less = True
            elif self_value > other_value:
                other_less = True

        if self_less and not other_less:
            return -1  # self < other
        elif other_less and not self_less:
            return 1  # self > other
        else:
            return 0  # concurrent or equal

    def __str__(self):
        return str(self.clock)