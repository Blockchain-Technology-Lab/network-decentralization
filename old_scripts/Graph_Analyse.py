import pandas as pd
import networkx as nx
from sqlalchemy import create_engine

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

#Generates some statistical analyses
def analyze_graph(G):
    n = len(G.nodes())
    m = len(G.edges())
    
    print(f"Number of nodes (n): {n}")
    print(f"Number of edges (m): {m}")
    
    if n < 2:
        print("The graph is too small to be complete.")
        return
    
    #Calculates the average degree of the nodes
    avg_degree = sum(dict(G.degree()).values()) / n
    print(f"Average degree: {avg_degree}")
    
    #Calculate the density 
    density = nx.density(G)
    print(f"Graph density: {density}")
    
    # Verify if it's a compelte graph or not 
    complete_edges = n * (n - 1) // 2  # Ensure an integer division
    print(f"Complete graph would have {complete_edges} edges.")
    
    # We calculate the proportion 
    proportion_actual = m / complete_edges
    proportion_expected = 1.0  # For a complete graph
    print(proportion_actual)

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

    # Analyze the graph
    analyze_graph(G)
