import requests
from ip2geotools.databases.noncommercial import DbIpCity # pip package: ip2geotools
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
    port=db_port
)

cur = conn.cursor()

# Get all nodes address from the database
cur.execute("SELECT node_id, ip_address FROM full_nodes")
nodes = cur.fetchall()

for data in nodes:
    node_id = data[0]
    ip_address = data[1]
    ip_address = str(ip_address)    
    res = DbIpCity.get(ip_address, api_key='free')
    localisation = f"{res.city}, {res.region}, {res.country}"
    print(ip_address, localisation)
    print(localisation)
    print(node_id)
    time.sleep(2)
    cur.execute("""
        UPDATE full_nodes
        SET location = %s
        WHERE ip_address = %s AND node_id = %s;
    """, (localisation, ip_address, node_id))

# Commit the changes
conn.commit()

# Close the connection to the database
cur.close()
conn.close()
