class Quorum:
    def __init__(self, N, R, W):
        self.N = N  # Total number of replicas
        self.R = R  # Read quorum
        self.W = W  # Write quorum

    def is_strong_consistency(self):
        return self.R + self.W > self.N

    def is_eventual_consistency(self):
        return self.R + self.W <= self.N

    def get_quorum_info(self):
        return {
            "N": self.N,
            "R": self.R,
            "W": self.W,
            "strong_consistency": self.is_strong_consistency(),
            "eventual_consistency": self.is_eventual_consistency(),
        }