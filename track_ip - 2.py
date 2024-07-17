import requests
from ip2geotools.databases.noncommercial import DbIpCity  # pip package: ip2geotools
import psycopg2
import time

# Database connection parameters
db_name = "Bitcoin_Full_nodes"
db_user = "bitcoin_user"
db_password = "root"
db_host = "localhost"
db_port = "5432"

# Connect to the database
conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port,
)
cur = conn.cursor()

# Get all nodes address from the database that do not have a location set
cur.execute("SELECT node_id, ip_address FROM full_nodes WHERE location IS NULL")
nodes = cur.fetchall()

total_nodes = len(nodes)
print(f"Total nodes to update: {total_nodes}")

for index, data in enumerate(nodes):
    node_id = data[0]
    ip_address = data[1]
    ip_address = str(ip_address)
    try:
        res = DbIpCity.get(ip_address, api_key='free')
        localisation = f"{res.city}, {res.region}, {res.country}"
        print(f"IP Address: {ip_address}, Location: {localisation}, Node ID: {node_id}")
        
        # Update the location for the current record
        cur.execute("""
            UPDATE full_nodes
            SET location = %s
            WHERE ip_address = %s AND node_id = %s;
        """, (localisation, ip_address, node_id))
        print(f"Updated location for IP {ip_address} and node_id {node_id}")
        
        # Commit the transaction after each update
        conn.commit()

        # Provide progress feedback
        if (index + 1) % 100 == 0:  # Log progress every 100 updates
            print(f"Progress: {index + 1}/{total_nodes} nodes updated")
        
        # Slow down the loop to avoid rate limiting
        time.sleep(2)

    except Exception as e:
        print(f"Error processing IP {ip_address}: {e}")

# Final commit to make sure all transactions are closed
conn.commit()
print("All changes committed to the database.")

# Close the connection to the database
cur.close()
conn.close()
print("Connection closed.")
print("All nodes updated successfully.")
