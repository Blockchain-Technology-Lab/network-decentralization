import os
import glob

def load_addresses_from_file(filename):
    #Load IP addresses from a file and return them as a set. --> In Python a SET only contains unique data.
    try:
        with open(filename, "r") as f:
            addresses = set(line.strip() for line in f)
        return addresses
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return set()

def compare_accumulative(file_list):
    #Compare IP addresses cumulatively across a list of files.
    if not file_list:
        print("No files to compare.")
        return

    # Start with empty set for cumulative addresses
    cumulative_addresses = set()
    
    # Iterate through the files and compare cumulatively
    for i, file in enumerate(file_list):
        file_addresses = load_addresses_from_file(file)
        
        # Calculate the percentage of addresses in the current file that are already known
        if file_addresses:
            known_count = len(cumulative_addresses.intersection(file_addresses))
            known_percentage = round((known_count / len(file_addresses)) * 100, 2)
        else:
            known_percentage = 100.0
        #Print the percentage of known addresses 
        print(f"Comparison with cumulative up to {file}: {known_percentage}% addresses known")
        # Print the total number of unique addresses in the cumulative set
        print(f"Total unique addresses so far: {len(cumulative_addresses)}")
        
        # Update the cumulative set with new addresses
        cumulative_addresses.update(file_addresses)
            

def main():
    # List all files that match the pattern sorted by creation/modification time
    files = sorted(glob.glob("addresses_*.txt"), key=os.path.getmtime, reverse=False)
    if len(files) < 1:
        print("Not enough files to compare.")
        return

    print(f"Comparing {len(files)} files")
    # Perform cumulative comparison
    compare_accumulative(files)

if __name__ == '__main__':
    main()
