import os
import xml.etree.ElementTree as ET
import mysql.connector
from datetime import datetime
import uuid
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_pid_from_filename(cursor, filename):
    # Extract fname, lname, and referrerID from the filename
    parts = filename.split('_')
    if len(parts) < 2:
        raise ValueError(f"Filename {filename} does not match expected format")

    fname = parts[0]
    lname = parts[1]
    referrerID = parts[2].split('.')[0]

    # Query the patient_data table
    query = """
    SELECT pid FROM patient_data 
    WHERE fname = %s AND lname = %s
    AND referrerID = %s;
    """

    cursor.execute(query, (fname, lname, referrerID))
    result = cursor.fetchone()

    if result is None:
        raise ValueError(f"No patient found for {fname} {lname}")

    return result[0]

def parse_ccda(file_path):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Define the namespace
        ns = {'cda': 'urn:hl7-org:v3'}

        # Find all sections
        sections = root.findall('.//cda:section', ns)

        encounters = []

        for section in sections:
            # Check if this is the Encounters section
            code = section.find('cda:code', ns)
            if code is not None and code.get('code') == '46240-8':
                # Find all encounter entries
                entries = section.findall('.//cda:encounter', ns)

                for entry in entries:
                    # Extract the encounter description
                    code_element = entry.find('.//cda:code', ns)
                    description = code_element.get('displayName') if code_element is not None else "Unknown"

                    # Extract the encounter dates
                    effective_time = entry.find('cda:effectiveTime', ns)
                    if effective_time is not None:
                        low = effective_time.find('cda:low', ns)
                        high = effective_time.find('cda:high', ns)
                        start_date = low.get('value') if low is not None else "Unknown"
                        end_date = high.get('value') if high is not None else "Unknown"

                        # Convert dates to a more readable format
                        if start_date != "Unknown":
                            start_date = datetime.strptime(start_date, "%Y%m%d%H%M%S").strftime("%Y-%m-%d")
                        if end_date != "Unknown":
                            end_date = datetime.strptime(end_date, "%Y%m%d%H%M%S").strftime("%Y-%m-%d")
                    else:
                        start_date = end_date = "Unknown"

                    encounters.append({
                        "description": description,
                        "start_date": start_date,
                        "end_date": end_date
                    })

        print(encounters)

        return encounters

    except ET.ParseError as e:
        logging.error(f"XML parsing error in file {file_path}: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Error processing CCDA file {file_path}: {str(e)}")
        raise

def parse_clinical_note(file_path):
    with open(file_path, 'r') as file:
        content = file.read()

    # Split the content into date-separated notes
    notes = {}
    current_date = None
    current_content = []

    for line in content.split('\n'):
        if line.strip() and len(line.split('-')) == 3:  # Assume this is a date
            if current_date:
                notes[current_date] = '\n'.join(current_content)
            current_date = datetime.strptime(line.strip(), "%Y-%m-%d")
            current_content = []
        else:
            current_content.append(line)

    if current_date:
        notes[current_date] = '\n'.join(current_content)

    return notes

# def insert_into_forms(cursor, date, encounter, pid, form_name='Clinical Notes'):
#     query = """
#     INSERT INTO forms (date, encounter, form_name, pid, user, groupname, authorized, formdir)
#     VALUES (%s, %s, %s, %s, 'admin', 'Default', 1, 'clinical_notes')
#     """
#     cursor.execute(query, (date, encounter, form_name, pid))
#     return cursor.lastrowid
#
# def insert_into_form_clinical_notes(cursor, form_id, date, pid, encounter, description):
#     query = """
#     INSERT INTO form_clinical_notes (form_id, uuid, date, pid, encounter, user, groupname,
#                                      authorized, activity, description, clinical_notes_type)
#     VALUES (%s, %s, %s, %s, %s, 'admin', 'Default', 1, 1, %s, 'Clinical Note')
#     """
#     note_uuid = uuid.uuid4().bytes
#     cursor.execute(query, (form_id, note_uuid, date, pid, encounter, description))

def process_files(ccda_folder, notes_folder, db_config):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    total_files = len([f for f in os.listdir(ccda_folder) if f.endswith('.xml')])
    processed_files = 0
    skipped_files = 0

    for ccda_file in os.listdir(ccda_folder):
        if ccda_file.endswith('.xml'):
            ccda_path = os.path.join(ccda_folder, ccda_file)
            try:
                # Get pid from filename
                pid = get_pid_from_filename(cursor, ccda_file)

                encounter_data = parse_ccda(ccda_path)

                # Assume the notes file has the same name but with a .txt extension
                notes_file = ccda_file.replace('.xml', '.txt')
                notes_path = os.path.join(notes_folder, notes_file)

                if os.path.exists(notes_path):
                    notes = parse_clinical_note(notes_path)

                    for date, encounter_id in encounter_data:
                        if date in notes:
                            print(date, encounter_id, pid, notes[date])
                            # Insert into forms table
                            #form_id = insert_into_forms(cursor, date, encounter_id, pid)

                            # Insert into form_clinical_notes table
                            #insert_into_form_clinical_notes(cursor, form_id, date.date(), pid, encounter_id, notes[date])

                processed_files += 1
                if processed_files % 100 == 0:  # Log progress every 100 files
                    logging.info(f"Processed {processed_files}/{total_files} files")

            except Exception as e:
                logging.error(f"Error processing file {ccda_file}: {str(e)}")
                skipped_files += 1

    conn.commit()
    cursor.close()
    conn.close()

    logging.info(f"Processing complete. Total files: {total_files}, Processed: {processed_files}, Skipped: {skipped_files}")


# Configuration
db_config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'root',
    'database': 'openemr'
}

ccda_folder = '/home/sunbiz/test_data/ccda'
notes_folder = '/home/sunbiz/test_data/notes'

# Run the processing
process_files(ccda_folder, notes_folder, db_config)