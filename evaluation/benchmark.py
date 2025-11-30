"""
Benchmark suite for evaluating Chord performance and consistency trade-offs.
"""

import asyncio
import time
import statistics
from typing import List, Dict, Tuple
import logging


class BenchmarkResults:
    """Container for benchmark results."""
    
    def __init__(self):
        self.latencies: List[float] = []
        self.throughputs: List[float] = []
        self.success_rates: List[float] = []
        self.consistency_level: str = ""
        self.config: Dict = {}
    
    def add_latency(self, latency: float):
        """Add a latency measurement (in ms)."""
        self.latencies.append(latency)
    
    def add_throughput(self, throughput: float):
        """Add a throughput measurement (ops/sec)."""
        self.throughputs.append(throughput)
    
    def add_success_rate(self, rate: float):
        """Add a success rate measurement (0-1)."""
        self.success_rates.append(rate)
    
    def get_stats(self) -> Dict:
        """Get statistical summary."""
        return {
            'latency': {
                'mean': statistics.mean(self.latencies) if self.latencies else 0,
                'median': statistics.median(self.latencies) if self.latencies else 0,
                'stdev': statistics.stdev(self.latencies) if len(self.latencies) > 1 else 0,
                'p95': self._percentile(self.latencies, 0.95) if self.latencies else 0,
                'p99': self._percentile(self.latencies, 0.99) if self.latencies else 0,
            },
            'throughput': {
                'mean': statistics.mean(self.throughputs) if self.throughputs else 0,
                'max': max(self.throughputs) if self.throughputs else 0,
            },
            'success_rate': {
                'mean': statistics.mean(self.success_rates) if self.success_rates else 0,
            },
            'config': self.config,
            'consistency': self.consistency_level,
        }
    
    def _percentile(self, data: List[float], p: float) -> float:
        """Calculate percentile."""
        sorted_data = sorted(data)
        index = int(len(sorted_data) * p)
        return sorted_data[min(index, len(sorted_data) - 1)]


class ChordBenchmark:
    """Benchmark suite for Chord DHT."""
    
    def __init__(self, n_replicas: int, read_quorum: int, write_quorum: int):
        """
        Initialize benchmark.
        
        Args:
            n_replicas: Number of replicas (N)
            read_quorum: Read quorum size (R)
            write_quorum: Write quorum size (W)
        """
        self.n_replicas = n_replicas
        self.read_quorum = read_quorum
        self.write_quorum = write_quorum
        
        self.logger = logging.getLogger("Benchmark")
        
        # Determine consistency level
        if read_quorum + write_quorum > n_replicas:
            self.consistency = "STRONG"
        elif read_quorum + write_quorum == n_replicas:
            self.consistency = "MODERATE"
        else:
            self.consistency = "EVENTUAL"
    
    async def benchmark_write_latency(self, num_operations: int = 100) -> BenchmarkResults:
        """
        Benchmark write operation latency.
        
        Args:
            num_operations: Number of write operations
            
        Returns:
            BenchmarkResults with latency measurements
        """
        self.logger.info(f"Benchmarking write latency ({num_operations} ops)")
        
        results = BenchmarkResults()
        results.consistency_level = self.consistency
        results.config = {
            'N': self.n_replicas,
            'R': self.read_quorum,
            'W': self.write_quorum,
        }
        
        for i in range(num_operations):
            start = time.time()
            
            # Simulate write operation
            await self._simulate_write(f"key{i}", f"value{i}")
            
            end = time.time()
            latency_ms = (end - start) * 1000
            results.add_latency(latency_ms)
        
        return results
    
    async def benchmark_read_latency(self, num_operations: int = 100) -> BenchmarkResults:
        """
        Benchmark read operation latency.
        
        Args:
            num_operations: Number of read operations
            
        Returns:
            BenchmarkResults with latency measurements
        """
        self.logger.info(f"Benchmarking read latency ({num_operations} ops)")
        
        results = BenchmarkResults()
        results.consistency_level = self.consistency
        results.config = {
            'N': self.n_replicas,
            'R': self.read_quorum,
            'W': self.write_quorum,
        }
        
        for i in range(num_operations):
            start = time.time()
            
            # Simulate read operation
            await self._simulate_read(f"key{i}")
            
            end = time.time()
            latency_ms = (end - start) * 1000
            results.add_latency(latency_ms)
        
        return results
    
    async def benchmark_throughput(self, duration_seconds: int = 10) -> BenchmarkResults:
        """
        Benchmark system throughput.
        
        Args:
            duration_seconds: Benchmark duration
            
        Returns:
            BenchmarkResults with throughput measurements
        """
        self.logger.info(f"Benchmarking throughput ({duration_seconds}s)")
        
        results = BenchmarkResults()
        results.consistency_level = self.consistency
        results.config = {
            'N': self.n_replicas,
            'R': self.read_quorum,
            'W': self.write_quorum,
        }
        
        start_time = time.time()
        total_ops = 0
        
        while time.time() - start_time < duration_seconds:
            # Perform mixed workload
            tasks = []
            for i in range(10):
                if i % 2 == 0:
                    tasks.append(self._simulate_write(f"key{i}", f"value{i}"))
                else:
                    tasks.append(self._simulate_read(f"key{i}"))
            
            await asyncio.gather(*tasks)
            total_ops += len(tasks)
            
            # Calculate instantaneous throughput
            elapsed = time.time() - start_time
            throughput = total_ops / elapsed
            results.add_throughput(throughput)
        
        return results
    
    async def _simulate_write(self, key: str, value: str):
        """Simulate a write operation with quorum."""
        # Simulate network delay based on quorum size
        delay = 0.001 * self.write_quorum  # 1ms per quorum member
        await asyncio.sleep(delay)
    
    async def _simulate_read(self, key: str):
        """Simulate a read operation with quorum."""
        # Simulate network delay based on quorum size
        delay = 0.001 * self.read_quorum  # 1ms per quorum member
        await asyncio.sleep(delay)


async def run_benchmarks():
    """
    Run comprehensive benchmarks with different quorum configurations.
    
    Returns:
        Dictionary of configuration -> BenchmarkResults
    """
    logging.basicConfig(level=logging.INFO)
    
    results = {}
    
    # Test different quorum configurations
    configs = [
        (3, 1, 1),  # Eventual consistency
        (3, 2, 1),  # Moderate
        (3, 2, 2),  # Strong
        (3, 3, 1),  # Strong (read-heavy)
        (3, 1, 3),  # Strong (write-heavy)
        (5, 3, 3),  # Strong with more replicas
    ]
    
    for n, r, w in configs:
        config_name = f"N={n},R={r},W={w}"
        print(f"\nBenchmarking configuration: {config_name}")
        print("="*60)
        
        benchmark = ChordBenchmark(n, r, w)
        
        # Run benchmarks
        write_latency = await benchmark.benchmark_write_latency(num_operations=100)
        read_latency = await benchmark.benchmark_read_latency(num_operations=100)
        throughput = await benchmark.benchmark_throughput(duration_seconds=5)
        
        # Combine results
        combined = BenchmarkResults()
        combined.consistency_level = benchmark.consistency
        combined.config = {'N': n, 'R': r, 'W': w}
        combined.latencies = write_latency.latencies + read_latency.latencies
        combined.throughputs = throughput.throughputs
        
        results[config_name] = combined
        
        # Print summary
        stats = combined.get_stats()
        print(f"Consistency: {stats['consistency']}")
        print(f"Write Latency: {stats['latency']['mean']:.2f} ms (p95: {stats['latency']['p95']:.2f} ms)")
        print(f"Throughput: {stats['throughput']['mean']:.2f} ops/s")
    
    return results


if __name__ == "__main__":
    # Run benchmarks
    results = asyncio.run(run_benchmarks())
    
    print("\n" + "="*60)
    print("Benchmark Summary")
    print("="*60)
    
    for config, result in results.items():
        stats = result.get_stats()
        print(f"\n{config} ({stats['consistency']}):")
        print(f"  Latency: {stats['latency']['mean']:.2f} ± {stats['latency']['stdev']:.2f} ms")
        print(f"  Throughput: {stats['throughput']['mean']:.2f} ops/s")
