# main.py
from api_client import get_all_skus
from utils import filter_records, count_records, save_to_file
from datetime import datetime

def main():
    # Retrieve records from the API
    all_records = get_all_skus()
    
    # Filter records created on or after January 1, 2022
    date_threshold = datetime(2022, 1, 1).timestamp()
    filtered_records = filter_records(all_records, date_threshold)
    
    # Count filtered records
    filtered_count = count_records(filtered_records)
    print(f"Count of filtered records: {filtered_count}")
    
    # Save filtered records to a file
    save_to_file(filtered_records, 'filtered_records.json')
    
if __name__ == "__main__":
    main()
