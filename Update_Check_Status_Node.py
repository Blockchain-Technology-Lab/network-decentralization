import os
import psycopg2
import subprocess
from datetime import datetime
import concurrent.futures

# Database configuration
db_name = "Bitcoin_Full_nodes"
db_user = "postgres"
db_password = "root"
db_host = "localhost"
db_port = "5432"

# Function to ping a node and return results
def ping_node(node):
    node_id, node_ip = node
    last_ping = datetime.now()
    
    # Ping the node and set status
    try:
        response = subprocess.check_output(["ping", "-c", "1", "-W", "1", node_ip], timeout=1).decode()
        if "time=" in response:
            return (node_id, node_ip, True, last_ping)
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        pass
    
    return (node_id, node_ip, False, last_ping)

# Connect to the database
conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)
cur = conn.cursor()

# Get all nodes from the database
cur.execute("SELECT node_id, ip_address FROM full_nodes")
nodes = cur.fetchall()

# Use ThreadPoolExecutor to ping nodes in parallel
with concurrent.futures.ThreadPoolExecutor(max_workers=20000) as executor:
    results = list(executor.map(ping_node, nodes))

# Update information in the full_nodes table and insert into node_status_history
record_time = datetime.now()

for node_id, node_ip, node_status, last_ping in results:
    # Update full_nodes table
    cur.execute("""
        UPDATE full_nodes
        SET status = %s, last_ping = %s
        WHERE node_id = %s
    """, (node_status, last_ping, node_id))
    
    # Insert into node_status_history table
    cur.execute("""
        INSERT INTO node_status_history (node_id, ip_address, status, last_ping, record_time)
        VALUES (%s, %s, %s, %s, %s)
    """, (node_id, node_ip, node_status, last_ping, record_time))

# Commit the changes
conn.commit()

# Close the connection to the database
cur.close()
conn.close()
