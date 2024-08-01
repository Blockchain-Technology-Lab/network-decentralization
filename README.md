# Network-decentralization
# EDI_Project_Information : 
This document contain different informations about the database and all the work and study did on the Bitcoin P2P Network. 
# Get_address_between_2_nodes.py : 
This script will simulate a "get address" message to a specific node (indicated in the main function) and print the result.  
The process is the following : 
- Send "Version" message and wait for an "version acknoledge" message
- Send "Get Address" message and wait for an "Address" message
Documentation concerning the frame of the message  :
https://developer.bitcoin.org/reference/p2p_networking.html 
# Graph_Analyse.py : 
This script will generate different statistics from the "know relationship" between the nodes from the data in the Postgre database (node_connections table). The script use the "NetworkX" python library. 
The process is the following : 
- Retrieve the data from the node_connections table.
- Generate a graph with NetworkX
- Generate some statistics about the graph (Average degree, density, the gaph type)
# Graph_degree_distribution.py
This script is the same script do the same thing as "the Graph_Analyse.py" script but it will also generate a graph in jpg representig the Degree distrubition of the nodes in the database (using NetworkX and matplotlib).
# Populate_DB_Node_Connections_Automated.py : 
This script will browse the "Full_node" table from the database and then for each node it will send a "get address" and then use the result to populate the "Node_connections" table. 
# Populate_DB_getnodeaddress.py : 
This script will use the "Bitcoin-cli getnodeaddress" to retrieve all the node known by our Full-node and then populate the "Full_nodes" table of the database.
# Update_Check_Status_Node.py : 
This script will send a ping to each node on the database to see the status of the nodes and then store the result in the database into the "node_status_history" table.
# Get_address_Export.py 
This script will send “Get address” to a specific node and collect the results of the “Addr” message in a .txt file. We need to execute this script as much as possible to collect the more data possible. we can do that in ubuntu with the bash command: 
for ((i=1; i<=5; i++)); do python3 Get_address_Export.py; echo "Finished iteration $i"; if [ $i -lt 10 ]; then sleep 20; fi; done
(This will execute automatically the script X times --> In this case 5) 
# Compare_Convergence.py 
This script will compare all the .txt files returned from the “Get_address_Export” script to evaluate the convergence and the number of known IP addresses of the node.
# Network_Geo_Decentralization_Graph.py
This script will generate a pie graph representing the number of nodes per country using the data from the local PostgreSQL database.

