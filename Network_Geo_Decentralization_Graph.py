import psycopg2
import matplotlib.pyplot as plt

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

# Query to get the location of all nodes
cur.execute("SELECT location FROM full_nodes WHERE location IS NOT NULL")
locations = cur.fetchall()

# Close the database connection
cur.close()
conn.close()

# Process locations to count nodes per country
country_count = {}

for loc in locations:
    # Assuming the location format is "city, region, country"
    try:
        country = loc[0].split(",")[-1].strip()
        if country in country_count:
            country_count[country] += 1
        else:
            country_count[country] = 1
    except:
        pass

# Total number of nodes
total_nodes = sum(country_count.values())

# If a country has a result below 2% we put it in the "Others" part. 
threshold = 0.02 * total_nodes
other_count = sum(count for country, count in country_count.items() if count < threshold)
country_count = {country: count for country, count in country_count.items() if count >= threshold}
if other_count > 0:
    country_count["Other"] = other_count

# Prepare data for plotting
countries = list(country_count.keys())
counts = list(country_count.values())

# Sort the countries by count
sorted_indices = sorted(range(len(counts)), key=lambda k: counts[k], reverse=True)
sorted_countries = [countries[i] for i in sorted_indices]
sorted_counts = [counts[i] for i in sorted_indices]

# Plotting the data using Matplotlib
plt.figure(figsize=(12, 8))
plt.pie(sorted_counts, labels=sorted_countries, autopct='%1.1f%%', startangle=140, textprops={'fontsize':8})
plt.title('Number of Bitcoin Nodes per Country')

# Add total number of nodes as text at the bottom of the graph
plt.figtext(0.5, 0.01, f'Total nodes analyzed: {total_nodes}', ha='center', fontsize=10)

# Save the plot as a .jpg
plt.savefig('bitcoin_nodes_per_country.jpg', format='jpg', dpi=300)

# plt.show()
