#!/usr/bin/env python3
"""
Generate architecture diagrams for the Snowpipe Streaming solution
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import Rectangle, FancyBboxPatch, Circle
import numpy as np

def create_architecture_diagram():
    """Create a comprehensive architecture diagram"""
    
    fig, ax = plt.subplots(1, 1, figsize=(16, 12))
    ax.set_xlim(0, 16)
    ax.set_ylim(0, 12)
    ax.axis('off')
    
    # Define colors
    colors = {
        'data_source': '#E3F2FD',
        'processing': '#FFF3E0', 
        'storage': '#E8F5E8',
        'monitoring': '#F3E5F5',
        'infrastructure': '#FFF9C4'
    }
    
    # Title
    ax.text(8, 11.5, 'Snowpipe Streaming High-Performance Architecture\nMTA Real-time Transit Data Pipeline', 
            ha='center', va='center', fontsize=16, fontweight='bold')
    
    # Data Sources Layer
    data_sources = FancyBboxPatch((0.5, 9), 3, 1.5, 
                                  boxstyle="round,pad=0.1", 
                                  facecolor=colors['data_source'], 
                                  edgecolor='black', linewidth=1)
    ax.add_patch(data_sources)
    ax.text(2, 9.75, 'Data Sources', ha='center', va='center', fontsize=12, fontweight='bold')
    ax.text(2, 9.4, '• MTA Real-time APIs\n• GTFS-RT Feeds\n• Multiple Endpoints', 
            ha='center', va='center', fontsize=9)
    
    # Processing Layer
    processing = FancyBboxPatch((5, 8), 6, 3, 
                               boxstyle="round,pad=0.1", 
                               facecolor=colors['processing'], 
                               edgecolor='black', linewidth=1)
    ax.add_patch(processing)
    ax.text(8, 10.5, 'Snowpipe Streaming Application', ha='center', va='center', 
            fontsize=12, fontweight='bold')
    
    # Sub-components within processing
    components = [
        (5.5, 9.5, 'Data\nValidation'),
        (7, 9.5, 'Transformation\n& Enrichment'),
        (8.5, 9.5, 'Quality\nChecks'),
        (10, 9.5, 'Error\nHandling'),
        (6.5, 8.5, 'Parallel Streaming Channels'),
        (9, 8.5, 'Batch Processing')
    ]
    
    for x, y, label in components:
        comp_box = FancyBboxPatch((x-0.4, y-0.3), 0.8, 0.6, 
                                 boxstyle="round,pad=0.05", 
                                 facecolor='white', 
                                 edgecolor='gray', linewidth=0.5)
        ax.add_patch(comp_box)
        ax.text(x, y, label, ha='center', va='center', fontsize=8)
    
    # Storage Layer (Snowflake)
    storage = FancyBboxPatch((12.5, 8.5), 3, 2, 
                            boxstyle="round,pad=0.1", 
                            facecolor=colors['storage'], 
                            edgecolor='black', linewidth=1)
    ax.add_patch(storage)
    ax.text(14, 10, 'Snowflake', ha='center', va='center', fontsize=12, fontweight='bold')
    ax.text(14, 9.5, '• Iceberg Tables\n• TRANSCOM_TSPANNICEBERG_EXTVOL\n• Clustered Storage', 
            ha='center', va='center', fontsize=9)
    ax.text(14, 8.8, 'DEMO.DEMO.ICYMTA', ha='center', va='center', fontsize=10, 
            fontweight='bold', style='italic')
    
    # Monitoring Layer
    monitoring = FancyBboxPatch((0.5, 5.5), 7, 2, 
                               boxstyle="round,pad=0.1", 
                               facecolor=colors['monitoring'], 
                               edgecolor='black', linewidth=1)
    ax.add_patch(monitoring)
    ax.text(4, 7, 'Monitoring & Observability', ha='center', va='center', 
            fontsize=12, fontweight='bold')
    
    monitoring_components = [
        (1.5, 6.2, 'Prometheus\nMetrics'),
        (3, 6.2, 'Grafana\nDashboards'),
        (4.5, 6.2, 'Alertmanager'),
        (6, 6.2, 'Health\nChecks'),
        (3.5, 5.8, 'Structured Logging')
    ]
    
    for x, y, label in monitoring_components:
        mon_box = FancyBboxPatch((x-0.4, y-0.25), 0.8, 0.5, 
                                boxstyle="round,pad=0.05", 
                                facecolor='white', 
                                edgecolor='purple', linewidth=0.5)
        ax.add_patch(mon_box)
        ax.text(x, y, label, ha='center', va='center', fontsize=8)
    
    # Analytics Layer
    analytics = FancyBboxPatch((9, 5.5), 6.5, 2, 
                              boxstyle="round,pad=0.1", 
                              facecolor=colors['storage'], 
                              edgecolor='black', linewidth=1)
    ax.add_patch(analytics)
    ax.text(12.25, 7, 'Analytics & Optimization', ha='center', va='center', 
            fontsize=12, fontweight='bold')
    
    analytics_components = [
        (10, 6.2, 'Materialized\nViews'),
        (11.5, 6.2, 'Performance\nTuning'),
        (13, 6.2, 'Auto\nClustering'),
        (14.5, 6.2, 'Query\nOptimization')
    ]
    
    for x, y, label in analytics_components:
        anal_box = FancyBboxPatch((x-0.4, y-0.25), 0.8, 0.5, 
                                 boxstyle="round,pad=0.05", 
                                 facecolor='white', 
                                 edgecolor='green', linewidth=0.5)
        ax.add_patch(anal_box)
        ax.text(x, y, label, ha='center', va='center', fontsize=8)
    
    # Infrastructure Layer
    infrastructure = FancyBboxPatch((0.5, 2.5), 15, 2.5, 
                                   boxstyle="round,pad=0.1", 
                                   facecolor=colors['infrastructure'], 
                                   edgecolor='black', linewidth=1)
    ax.add_patch(infrastructure)
    ax.text(8, 4.5, 'Infrastructure & Deployment', ha='center', va='center', 
            fontsize=12, fontweight='bold')
    
    infra_components = [
        (2, 3.8, 'Docker\nContainers'),
        (4, 3.8, 'Load\nBalancer'),
        (6, 3.8, 'Redis\nCache'),
        (8, 3.8, 'Auto\nScaling'),
        (10, 3.8, 'Health\nMonitoring'),
        (12, 3.8, 'Security\n& Auth'),
        (14, 3.8, 'Backup &\nRecovery'),
        (5, 3, 'High Availability'),
        (11, 3, 'Performance Optimization')
    ]
    
    for x, y, label in infra_components:
        if 'High Availability' in label or 'Performance' in label:
            width, height = 2, 0.4
        else:
            width, height = 0.8, 0.5
        
        infra_box = FancyBboxPatch((x-width/2, y-height/2), width, height, 
                                  boxstyle="round,pad=0.05", 
                                  facecolor='white', 
                                  edgecolor='orange', linewidth=0.5)
        ax.add_patch(infra_box)
        ax.text(x, y, label, ha='center', va='center', fontsize=8)
    
    # Data Flow Arrows
    # Data source to processing
    ax.arrow(3.5, 9.75, 1.3, 0, head_width=0.1, head_length=0.1, 
             fc='blue', ec='blue', alpha=0.7)
    ax.text(4.25, 10, 'Real-time\nData', ha='center', va='bottom', fontsize=8, color='blue')
    
    # Processing to storage
    ax.arrow(11, 9.5, 1.3, 0, head_width=0.1, head_length=0.1, 
             fc='green', ec='green', alpha=0.7)
    ax.text(11.75, 9.8, 'Streamed\nData', ha='center', va='bottom', fontsize=8, color='green')
    
    # Processing to monitoring
    ax.arrow(6.5, 8, -1.5, -0.3, head_width=0.1, head_length=0.1, 
             fc='purple', ec='purple', alpha=0.7)
    ax.text(5.5, 7.6, 'Metrics', ha='center', va='bottom', fontsize=8, color='purple')
    
    # Performance Metrics Box
    perf_metrics = FancyBboxPatch((0.5, 0.5), 7, 1.5, 
                                 boxstyle="round,pad=0.1", 
                                 facecolor='#FFEBEE', 
                                 edgecolor='red', linewidth=1)
    ax.add_patch(perf_metrics)
    ax.text(4, 1.7, 'Performance Characteristics', ha='center', va='center', 
            fontsize=11, fontweight='bold')
    ax.text(4, 1.2, '• Throughput: 10,000+ records/minute\n• Latency: <30 seconds\n• Availability: 99.9%', 
            ha='center', va='center', fontsize=9)
    
    # Technology Stack Box
    tech_stack = FancyBboxPatch((8.5, 0.5), 7, 1.5, 
                               boxstyle="round,pad=0.1", 
                               facecolor='#E1F5FE', 
                               edgecolor='blue', linewidth=1)
    ax.add_patch(tech_stack)
    ax.text(12, 1.7, 'Technology Stack', ha='center', va='center', 
            fontsize=11, fontweight='bold')
    ax.text(12, 1.2, '• Python 3.11+ • Snowflake • Docker\n• Prometheus • Grafana • Redis', 
            ha='center', va='center', fontsize=9)
    
    # Legend
    legend_elements = [
        mpatches.Patch(color=colors['data_source'], label='Data Sources'),
        mpatches.Patch(color=colors['processing'], label='Processing Layer'),
        mpatches.Patch(color=colors['storage'], label='Storage & Analytics'),
        mpatches.Patch(color=colors['monitoring'], label='Monitoring'),
        mpatches.Patch(color=colors['infrastructure'], label='Infrastructure')
    ]
    
    ax.legend(handles=legend_elements, loc='upper right', bbox_to_anchor=(0.98, 0.98))
    
    plt.tight_layout()
    plt.savefig('snowpipe_streaming_architecture.png', dpi=300, bbox_inches='tight')
    plt.savefig('snowpipe_streaming_architecture.pdf', bbox_inches='tight')
    print("Architecture diagram saved as snowpipe_streaming_architecture.png and .pdf")
    
    return fig

def create_data_flow_diagram():
    """Create a detailed data flow diagram"""
    
    fig, ax = plt.subplots(1, 1, figsize=(14, 10))
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 10)
    ax.axis('off')
    
    # Title
    ax.text(7, 9.5, 'MTA Data Flow Through Snowpipe Streaming', 
            ha='center', va='center', fontsize=16, fontweight='bold')
    
    # Flow stages
    stages = [
        (2, 8, "MTA APIs\n(GTFS-RT)", '#E3F2FD'),
        (2, 6.5, "Data\nIngestion", '#FFF3E0'),
        (2, 5, "Validation &\nTransformation", '#FFF3E0'),
        (6, 5, "Parallel\nChannels", '#FFECB3'),
        (10, 5, "Snowflake\nIceberg Table", '#E8F5E8'),
        (2, 3.5, "Error\nHandling", '#FFCDD2'),
        (6, 3.5, "Quality\nChecks", '#F3E5F5'),
        (10, 3.5, "Analytics\nViews", '#E8F5E8'),
        (2, 2, "Monitoring\n& Alerts", '#F3E5F5'),
        (6, 2, "Performance\nMetrics", '#F3E5F5'),
        (10, 2, "Dashboards\n& Reports", '#F3E5F5')
    ]
    
    # Draw stages
    for x, y, label, color in stages:
        box = FancyBboxPatch((x-0.8, y-0.5), 1.6, 1, 
                            boxstyle="round,pad=0.1", 
                            facecolor=color, 
                            edgecolor='black', linewidth=1)
        ax.add_patch(box)
        ax.text(x, y, label, ha='center', va='center', fontsize=10, fontweight='bold')
    
    # Data flow arrows with labels
    flows = [
        ((2, 7.5), (2, 7), "API Calls\n30s intervals"),
        ((2, 6), (2, 5.5), "Raw JSON\nData"),
        ((2, 4.5), (5.2, 4.5), "Validated\nRecords"),
        ((6.8, 5), (9.2, 5), "Batched\nStreaming"),
        ((2, 4.5), (2, 4), "Invalid\nRecords"),
        ((6, 4.5), (6, 4), "Quality\nMetrics"),
        ((10, 4.5), (10, 4), "Aggregated\nData"),
        ((2.8, 3.5), (5.2, 3.5), "Error\nReports"),
        ((2.8, 2), (5.2, 2), "System\nMetrics"),
        ((6.8, 2), (9.2, 2), "Performance\nData")
    ]
    
    for (start_x, start_y), (end_x, end_y), label in flows:
        ax.annotate('', xy=(end_x, end_y), xytext=(start_x, start_y),
                   arrowprops=dict(arrowstyle='->', lw=2, color='blue', alpha=0.7))
        
        # Add label at midpoint
        mid_x, mid_y = (start_x + end_x) / 2, (start_y + end_y) / 2
        ax.text(mid_x, mid_y + 0.2, label, ha='center', va='center', 
                fontsize=8, color='blue', bbox=dict(boxstyle="round,pad=0.2", 
                facecolor='white', edgecolor='blue', alpha=0.8))
    
    # Side panel with metrics
    metrics_box = FancyBboxPatch((11.5, 6), 2.3, 3, 
                                boxstyle="round,pad=0.1", 
                                facecolor='#F5F5F5', 
                                edgecolor='gray', linewidth=1)
    ax.add_patch(metrics_box)
    ax.text(12.65, 8.5, 'Key Metrics', ha='center', va='center', 
            fontsize=12, fontweight='bold')
    
    metrics_text = """
    • 10K+ records/min
    • <30s latency
    • 99.9% uptime
    • <1% error rate
    • 4 parallel channels
    • 1000 batch size
    • Real-time alerts
    """
    ax.text(12.65, 7.3, metrics_text, ha='center', va='center', fontsize=9)
    
    plt.tight_layout()
    plt.savefig('mta_data_flow_diagram.png', dpi=300, bbox_inches='tight')
    plt.savefig('mta_data_flow_diagram.pdf', bbox_inches='tight')
    print("Data flow diagram saved as mta_data_flow_diagram.png and .pdf")
    
    return fig

if __name__ == "__main__":
    print("Generating architecture diagrams...")
    
    # Create architecture diagram
    arch_fig = create_architecture_diagram()
    
    # Create data flow diagram
    flow_fig = create_data_flow_diagram()
    
    print("Diagrams generated successfully!")
    print("Files created:")
    print("  - snowpipe_streaming_architecture.png")
    print("  - snowpipe_streaming_architecture.pdf") 
    print("  - mta_data_flow_diagram.png")
    print("  - mta_data_flow_diagram.pdf")