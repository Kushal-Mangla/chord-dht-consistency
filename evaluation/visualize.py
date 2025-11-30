"""
Visualization tools for benchmark results.
"""

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from typing import Dict, List
import seaborn as sns


def plot_latency_comparison(results: Dict, output_file: str = "latency_comparison.png"):
    """
    Plot latency comparison across different quorum configurations.
    
    Args:
        results: Dictionary of config -> BenchmarkResults
        output_file: Output filename
    """
    configs = []
    mean_latencies = []
    p95_latencies = []
    consistency_levels = []
    
    for config_name, result in results.items():
        stats = result.get_stats()
        configs.append(config_name)
        mean_latencies.append(stats['latency']['mean'])
        p95_latencies.append(stats['latency']['p95'])
        consistency_levels.append(stats['consistency'])
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = np.arange(len(configs))
    width = 0.35
    
    # Plot bars
    bars1 = ax.bar(x - width/2, mean_latencies, width, label='Mean Latency', alpha=0.8)
    bars2 = ax.bar(x + width/2, p95_latencies, width, label='P95 Latency', alpha=0.8)
    
    # Color by consistency level
    colors = {'STRONG': 'red', 'MODERATE': 'orange', 'EVENTUAL': 'green'}
    for i, (bar1, bar2, level) in enumerate(zip(bars1, bars2, consistency_levels)):
        bar1.set_color(colors.get(level, 'blue'))
        bar2.set_color(colors.get(level, 'blue'))
        bar2.set_alpha(0.6)
    
    # Labels and title
    ax.set_xlabel('Configuration', fontsize=12, fontweight='bold')
    ax.set_ylabel('Latency (ms)', fontsize=12, fontweight='bold')
    ax.set_title('Latency vs. Quorum Configuration', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(configs, rotation=45, ha='right')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    print(f"Saved latency comparison to {output_file}")
    plt.close()


def plot_throughput_comparison(results: Dict, output_file: str = "throughput_comparison.png"):
    """
    Plot throughput comparison across different quorum configurations.
    
    Args:
        results: Dictionary of config -> BenchmarkResults
        output_file: Output filename
    """
    configs = []
    throughputs = []
    consistency_levels = []
    
    for config_name, result in results.items():
        stats = result.get_stats()
        configs.append(config_name)
        throughputs.append(stats['throughput']['mean'])
        consistency_levels.append(stats['consistency'])
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Color by consistency level
    colors = [{'STRONG': 'red', 'MODERATE': 'orange', 'EVENTUAL': 'green'}[level] 
              for level in consistency_levels]
    
    bars = ax.bar(configs, throughputs, color=colors, alpha=0.7)
    
    # Labels and title
    ax.set_xlabel('Configuration', fontsize=12, fontweight='bold')
    ax.set_ylabel('Throughput (ops/sec)', fontsize=12, fontweight='bold')
    ax.set_title('Throughput vs. Quorum Configuration', fontsize=14, fontweight='bold')
    ax.set_xticklabels(configs, rotation=45, ha='right')
    ax.grid(axis='y', alpha=0.3)
    
    # Add legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='green', alpha=0.7, label='Eventual'),
        Patch(facecolor='orange', alpha=0.7, label='Moderate'),
        Patch(facecolor='red', alpha=0.7, label='Strong'),
    ]
    ax.legend(handles=legend_elements, title='Consistency')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    print(f"Saved throughput comparison to {output_file}")
    plt.close()


def plot_consistency_tradeoff(results: Dict, output_file: str = "consistency_tradeoff.png"):
    """
    Plot latency vs. throughput trade-off, colored by consistency level.
    
    Args:
        results: Dictionary of config -> BenchmarkResults
        output_file: Output filename
    """
    data = []
    
    for config_name, result in results.items():
        stats = result.get_stats()
        data.append({
            'config': config_name,
            'latency': stats['latency']['mean'],
            'throughput': stats['throughput']['mean'],
            'consistency': stats['consistency'],
            'R': stats['config']['R'],
            'W': stats['config']['W'],
            'N': stats['config']['N'],
        })
    
    df = pd.DataFrame(data)
    
    # Create figure
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # Color by consistency level
    colors = {'STRONG': 'red', 'MODERATE': 'orange', 'EVENTUAL': 'green'}
    
    for consistency in ['EVENTUAL', 'MODERATE', 'STRONG']:
        subset = df[df['consistency'] == consistency]
        ax.scatter(subset['latency'], subset['throughput'], 
                  color=colors[consistency], s=200, alpha=0.6,
                  label=consistency, edgecolors='black', linewidths=1.5)
        
        # Add labels
        for _, row in subset.iterrows():
            ax.annotate(row['config'], 
                       (row['latency'], row['throughput']),
                       xytext=(5, 5), textcoords='offset points',
                       fontsize=8, alpha=0.8)
    
    # Labels and title
    ax.set_xlabel('Mean Latency (ms)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Mean Throughput (ops/sec)', fontsize=12, fontweight='bold')
    ax.set_title('Consistency-Performance Trade-off', fontsize=14, fontweight='bold')
    ax.legend(title='Consistency Level')
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    print(f"Saved consistency trade-off to {output_file}")
    plt.close()


def plot_quorum_heatmap(output_file: str = "quorum_heatmap.png"):
    """
    Plot heatmap showing consistency level for different R and W values.
    
    Args:
        output_file: Output filename
    """
    N = 5  # Number of replicas
    
    # Create matrix for consistency levels
    consistency_matrix = np.zeros((N, N))
    
    for r in range(1, N + 1):
        for w in range(1, N + 1):
            if r + w > N:
                consistency_matrix[r-1, w-1] = 2  # Strong
            elif r + w == N:
                consistency_matrix[r-1, w-1] = 1  # Moderate
            else:
                consistency_matrix[r-1, w-1] = 0  # Eventual
    
    # Create figure
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # Custom colormap
    cmap = plt.cm.colors.ListedColormap(['green', 'orange', 'red'])
    bounds = [-0.5, 0.5, 1.5, 2.5]
    norm = plt.cm.colors.BoundaryNorm(bounds, cmap.N)
    
    im = ax.imshow(consistency_matrix, cmap=cmap, norm=norm, aspect='auto')
    
    # Set ticks
    ax.set_xticks(np.arange(N))
    ax.set_yticks(np.arange(N))
    ax.set_xticklabels(range(1, N + 1))
    ax.set_yticklabels(range(1, N + 1))
    
    # Labels
    ax.set_xlabel('Write Quorum (W)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Read Quorum (R)', fontsize=12, fontweight='bold')
    ax.set_title(f'Consistency Levels (N={N})', fontsize=14, fontweight='bold')
    
    # Add text annotations
    for r in range(N):
        for w in range(N):
            level = int(consistency_matrix[r, w])
            text_labels = ['Eventual', 'Moderate', 'Strong']
            text = ax.text(w, r, text_labels[level],
                         ha="center", va="center", color="white",
                         fontsize=9, fontweight='bold')
    
    # Colorbar
    cbar = plt.colorbar(im, ax=ax, ticks=[0, 1, 2])
    cbar.ax.set_yticklabels(['Eventual', 'Moderate', 'Strong'])
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    print(f"Saved quorum heatmap to {output_file}")
    plt.close()


def plot_all_results(results: Dict, output_dir: str = "."):
    """
    Generate all plots for benchmark results.
    
    Args:
        results: Dictionary of config -> BenchmarkResults
        output_dir: Output directory for plots
    """
    import os
    
    print("\nGenerating visualizations...")
    print("="*60)
    
    # Create output directory if needed
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate plots
    plot_latency_comparison(results, f"{output_dir}/latency_comparison.png")
    plot_throughput_comparison(results, f"{output_dir}/throughput_comparison.png")
    plot_consistency_tradeoff(results, f"{output_dir}/consistency_tradeoff.png")
    plot_quorum_heatmap(f"{output_dir}/quorum_heatmap.png")
    
    print("\nAll visualizations saved!")


if __name__ == "__main__":
    # Generate example plots
    print("Generating example visualizations...")
    
    # Create dummy results
    from evaluation.benchmark import BenchmarkResults
    
    results = {}
    
    configs = [
        (3, 1, 1, "EVENTUAL"),
        (3, 2, 1, "MODERATE"),
        (3, 2, 2, "STRONG"),
    ]
    
    for n, r, w, consistency in configs:
        config_name = f"N={n},R={r},W={w}"
        result = BenchmarkResults()
        result.consistency_level = consistency
        result.config = {'N': n, 'R': r, 'W': w}
        
        # Dummy data (lower latency for eventual, higher throughput)
        if consistency == "EVENTUAL":
            result.latencies = [2.0 + np.random.random() for _ in range(100)]
            result.throughputs = [500 + np.random.random() * 50 for _ in range(10)]
        elif consistency == "MODERATE":
            result.latencies = [3.0 + np.random.random() for _ in range(100)]
            result.throughputs = [400 + np.random.random() * 50 for _ in range(10)]
        else:  # STRONG
            result.latencies = [4.0 + np.random.random() for _ in range(100)]
            result.throughputs = [300 + np.random.random() * 50 for _ in range(10)]
        
        results[config_name] = result
    
    plot_all_results(results, output_dir="plots")
