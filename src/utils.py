# utils.py
import json

def filter_records(records, date_threshold):
    filtered_records = [record for record in records if record['createdAt'] >= date_threshold]
    return filtered_records

def count_records(records):
    return len(records)

def save_to_file(records, filename):
    with open(filename, 'w') as file:
        json.dump(records, file, indent=4)
