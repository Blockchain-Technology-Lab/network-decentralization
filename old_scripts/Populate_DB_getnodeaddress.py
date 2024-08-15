import psycopg2
import json
import datetime
import subprocess


db_name = "Bitcoin_Full_nodes"
db_user = "bitcoin_user"
db_password = "root"
db_host = "localhost"
db_port = "5432"

conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)


cur = conn.cursor()

# Execute the bitcoin-cli getnodeaddresses 
result = subprocess.run(["/home/bitcoin/bitcoin/bin/bitcoin-cli", "getnodeaddresses", "0"], capture_output=True, text=True)

node_info = json.loads(result.stdout) #Parse the cli result and return a dictionnary named node_info 


for node in node_info: 
    ip_address = node["address"]
    port = node["port"]
    network = node["network"]
    last_seen = datetime.datetime.fromtimestamp(node["time"])
    status = True

    cur.execute("""
        INSERT INTO public."Full_nodes" (node_id, ip_address, port, network, status, last_seen)
        VALUES (nextval('public.full_nodes_node_id_seq'), %s, %s, %s, %s, %s)
        ON CONFLICT (ip_address, port, network) DO UPDATE SET node_id = EXCLUDED.node_id, status = %s, last_seen = %s;
    """, (ip_address, port, network, status, last_seen, status, last_seen))


conn.commit()

# Close the connexion
cur.close()
conn.close()
