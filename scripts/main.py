import subprocess
import query_data
import sys

def main():

    # Step 1: Download latest data
    print("\n--- Step 1: Checking for new data from TWSE ---")
    query_data.main() 

    # Step 2: Build tables (Process raw CSVs into Parquet)
    print(f"\n--- Step 2: Processing data ---")
    subprocess.run([sys.executable, "scripts/build_table.py"], check=True)

    # Step 3: Launch Dashboard
    subprocess.run([sys.executable, "scripts/app.py"], check=True)

if __name__ == "__main__":
    main()