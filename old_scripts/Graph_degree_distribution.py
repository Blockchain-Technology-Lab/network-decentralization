import pandas as pd
import networkx as nx
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

# Database connection 
def connect_db():
    engine = create_engine("postgresql+psycopg2://postgres:root@localhost:5432/Bitcoin_Full_nodes")
    return engine

# Retrieve data from the database
def fetch_data(engine):
    query_nodes = "SELECT node_id, ip_address FROM full_nodes;"
    query_connections = "SELECT source_node_id, destination_node_id FROM node_connections;"
    
    print("[INFO] Fetching data from the database.")
    nodes = pd.read_sql(query_nodes, engine)
    connections = pd.read_sql(query_connections, engine)
    
    print(f"[INFO] Number of nodes fetched: {len(nodes)}")
    print(f"[INFO] Number of connections fetched: {len(connections)}")
    
    return nodes, connections

# Create a graph with NetworkX 
def create_graph(nodes, connections):
    print("[INFO] Creating graph.")
    G = nx.Graph()
    
    # Manage nodes 
    for _, row in nodes.iterrows():
        G.add_node(row['node_id'], ip_address=row['ip_address'])
    
    # Manage vertices
    for _, row in connections.iterrows():
        G.add_edge(row['source_node_id'], row['destination_node_id'])
    
    # Print information 
    print(f"[INFO] Number of nodes in graph: {G.number_of_nodes()}")
    print(f"[INFO] Number of edges in graph: {G.number_of_edges()}")
    
    return G

# Verify if the graph is complete
def is_complete_graph(G):
    n = len(G.nodes())
    m = len(G.edges())
    total_possible_edges = n * (n - 1) // 2  # Ensure an integer division
    return m == total_possible_edges

# Function to format the y-axis labels as integers
def y_label_format(x, pos):
    return '%d' % x

# Function to plot degree histogram
def plot_degree_histogram(G, filename="degree_histogram.jpg"):
    print("[INFO] Generating degree histogram.")
    
    # Get degree histogram
    degree_hist = nx.degree_histogram(G)
    
    # Generate x and y axis values
    degrees = range(len(degree_hist))
    frequency = degree_hist

    # Plot the degree histogram
    plt.figure(figsize=(14, 8))  # Increased figure size for better readability
    plt.bar(degrees, frequency, width=0.80, color='b')
    plt.title("Degree Histogram")
    plt.xlabel("Degree")
    plt.ylabel("Frequency")
    plt.yscale('log')  # Use logarithmic scale for y-axis for better visibility if needed
    
    # Apply the custom formatter to y-axis
    formatter = FuncFormatter(y_label_format)
    plt.gca().yaxis.set_major_formatter(formatter)
    
    # Display grid
    plt.grid(True, which="both", ls="--")

    # Adjust x-axis limits if needed
    plt.xlim(0, max(degrees) + 1)

    # Save the plot as a JPG file
    plt.savefig(filename)
    print(f"[INFO] Degree histogram saved as {filename}")

# Generates statistics and plot degree histogram
def analyze_graph(G):
    n = len(G.nodes())
    m = len(G.edges())
    
    print(f"Number of nodes (n): {n}")
    print(f"Number of edges (m): {m}")

    if n < 2:
        print("The graph is too small to be complete.")
        return
    
    # Calculates the average degree of the nodes
    avg_degree = sum(dict(G.degree()).values()) / n
    print(f"Average degree: {avg_degree}")
    
    # Calculate the density 
    density = nx.density(G)
    print(f"Graph density: {density}")
    
    # Verify if it's a complete graph or not 
    complete_edges = n * (n - 1) // 2
    print(f"Complete graph would have {complete_edges} edges.")
    
    # We calculate the proportion 
    proportion_actual = m / complete_edges
    proportion_expected = 1.0  # For a complete graph
    print(f"Proportion of edges (actual): {proportion_actual}")
    
    # Plot and save the degree histogram
    plot_degree_histogram(G)

# Main execution
if __name__ == "__main__":
    engine = connect_db()
    
    # Fetch all nodes and connections from the database
    nodes, connections = fetch_data(engine)
    
    # Create the graph
    G = create_graph(nodes, connections)
    
    # Check if the graph is complete
    if is_complete_graph(G):
        print("The graph is complete.")
    else:
        print("The graph is sparse.")

    # Analyze the graph and generate histogram
    analyze_graph(G)
