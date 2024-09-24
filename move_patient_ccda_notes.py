import os
import shutil
import mysql.connector
from mysql.connector import Error
from collections import defaultdict

def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host="127.0.0.1",
            database="openemr",
            user="root",
            password="root"
        )
        return connection
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None

def get_patient_data(connection):
    try:
        cursor = connection.cursor(dictionary=True)
        query = "SELECT fname, lname, referrerID FROM patient_data"
        cursor.execute(query)
        return cursor.fetchall()
    except Error as e:
        print(f"Error fetching patient data: {e}")
        return []

def create_filename(fname, lname, referrerID):
    # Replace spaces with underscores in fname and lname
    fname_formatted = fname.replace(' ', '_')
    lname_formatted = lname.replace(' ', '_')
    return f"{fname_formatted}_{lname_formatted}_{referrerID}"

def move_patient_files(patient_data, source_folders, destination_folders):
    counters = {
        'moved': defaultdict(int),
        'errors': defaultdict(int),
        'not_found': defaultdict(int)
    }

    for patient in patient_data:
        fname = patient['fname']
        lname = patient['lname']
        referrerID = patient['referrerID']

        file_prefix = create_filename(fname, lname, referrerID)

        for extension, (source_folder, dest_folder) in destination_folders.items():
            filename = f"{file_prefix}{extension}"
            source_path = os.path.join(source_folder, filename)
            destination_path = os.path.join(dest_folder, filename)

            if os.path.exists(source_path):
                try:
                    shutil.copy(source_path, destination_path)
                    #print(f"Moved {filename} to {dest_folder}")
                    counters['moved'][extension] += 1
                except Exception as e:
                    print(f"Error moving {filename}: {e}")
                    counters['errors'][extension] += 1
            else:
                print(f"File not found: {source_path}")
                counters['not_found'][extension] += 1

    return counters

def print_summary(counters):
    print("\nSummary:")
    print("--------")
    for extension in set(counters['moved'].keys()) | set(counters['errors'].keys()) | set(counters['not_found'].keys()):
        print(f"\nFor {extension} files:")
        print(f"  Moved successfully: {counters['moved'][extension]}")
        print(f"  Errors during move: {counters['errors'][extension]}")
        print(f"  Files not found: {counters['not_found'][extension]}")

    total_moved = sum(counters['moved'].values())
    total_errors = sum(counters['errors'].values())
    total_not_found = sum(counters['not_found'].values())
    print(f"\nTotal files moved: {total_moved}")
    print(f"Total errors: {total_errors}")
    print(f"Total files not found: {total_not_found}")


connection = connect_to_database()
if not connection:
    print('Connection to MySQL failed')
    exit(1)

patient_data = get_patient_data(connection)
connection.close()

if not patient_data:
    print("No patient data found.")
    exit(1)

base_path = "/home/sunbiz/synthea/output"
destination_folders = {
    '.xml': (os.path.join(base_path, 'ccda'), '/home/sunbiz/11k_data/moved_ccda'),
    '.txt': (os.path.join(base_path, 'notes'), '/home/sunbiz/11k_data/moved_notes')
}

# Create destination folders if they don't exist
for _, dest_folder in destination_folders.values():
    os.makedirs(dest_folder, exist_ok=True)

counters = move_patient_files(patient_data, None, destination_folders)
print_summary(counters)