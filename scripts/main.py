import subprocess
import query_data

def main():

    # Step 1: Download latest data
    print("\n--- Step 1: Checking for new data from TWSE ---")
    query_data.main() 

    # Step 2: Build tables (Process raw CSVs into Parquet)
    print(f"\n--- Step 2: Processing data ---")
    subprocess.run(["python3", "scripts/build_table.py"])

    # Step 3: Launch Dashboard
    subprocess.run(["python3", "scripts/app.py"])

if __name__ == "__main__":
    main()