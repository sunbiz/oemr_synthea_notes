import os
import xml.etree.ElementTree as ET
import mysql.connector
from datetime import datetime
import uuid
import logging

logging.basicConfig(level=logging.CRITICAL, format='%(asctime)s - %(levelname)s - %(message)s')

def get_pid_from_filename(cursor, filename):
    # Remove the file extension
    name_parts = filename.rsplit('.', 1)[0].split('_')

    # The last part should be the referrerID
    referrerID = name_parts[-1]

    # The second to last part should be the last name
    lname = name_parts[-2]

    # Everything else is considered part of the first name
    fname = ' '.join(name_parts[:-2])

    if not fname or not lname or not referrerID:
        raise ValueError(f"Filename {filename} does not match expected format")

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

def get_encounter_from_pid_date(cursor, pid, date):
    # Query the patient_data table
    query = """
    SELECT encounter FROM form_encounter 
    WHERE pid = %s AND date LIKE %s;
    """

    cursor.execute(query, (pid, date + '%'))
    result = cursor.fetchone()

    if result is None:
        raise ValueError(f"No encounter found for {pid} {date}")

    return result[0]

def parse_ccda(cursor, pid, file_path):
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

                    try:
                        encounter = get_encounter_from_pid_date(cursor, pid, start_date)
                        encounters.append({
                            "date": start_date,
                            "encounter_id": encounter
                        })
                    except ValueError as e:
                        logging.warning(f"No encounter found for pid {pid} on date {start_date}: {str(e)}")
                        # Continue processing other encounters even if one fails

        return encounters

    except ET.ParseError as e:
        logging.error(f"XML parsing error in file {file_path}: {str(e)}")
        return []
    except Exception as e:
        logging.error(f"Error processing CCDA file {file_path}: {str(e)}")
        return []

def parse_clinical_note(file_path):
    with open(file_path, 'r') as file:
        content = file.read()

    # Split the content into date-separated notes
    notes = {}
    current_date = None
    current_content = []

    for line in content.split('\n'):
        if line.strip() and len(line.strip().split('-')) == 3 and len(line.strip())==10:  # Assume this is a date
            if current_date:
                notes[current_date] = '\n'.join(current_content)
            current_date = str(datetime.strptime(line.strip(), "%Y-%m-%d").date())
            current_content = []
        else:
            current_content.append(line)

    if current_date:
        notes[current_date] = '\n'.join(current_content)

    return notes

def insert_into_forms(cursor, date, encounter, pid, form_name='Clinical Notes'):
    query = """
    INSERT INTO forms (date, encounter, form_name, pid, user, groupname, authorized, formdir)
    VALUES (%s, %s, %s, %s, 'admin', 'Default', 1, 'clinical_notes')
    """
    cursor.execute(query, (date, encounter, form_name, pid))
    return cursor.lastrowid

def insert_into_form_clinical_notes(cursor, form_id, date, pid, encounter, description):
    query = """
    INSERT INTO form_clinical_notes (form_id, uuid, date, pid, encounter, user, groupname,
                                     authorized, activity, description, clinical_notes_type)
    VALUES (%s, %s, %s, %s, %s, 'admin', 'Default', 1, 1, %s, 'Clinical Note')
    """
    note_uuid = uuid.uuid4().bytes
    cursor.execute(query, (form_id, note_uuid, date, pid, encounter, description))

def process_files(ccda_folder, notes_folder, db_config):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(buffered=True)

    total_files = len([f for f in os.listdir(ccda_folder) if f.endswith('.xml')])
    processed_files = 0
    skipped_files = 0
    skip_reasons = {
        'no_pid': 0,
        'no_encounters': 0,
        'no_notes': 0,
        'other_error': 0
    }

    for ccda_file in os.listdir(ccda_folder):
        if ccda_file.endswith('.xml'):
            ccda_path = os.path.join(ccda_folder, ccda_file)
            try:
                # Get pid from filename
                try:
                    pid = get_pid_from_filename(cursor, ccda_file)
                except ValueError as e:
                    logging.warning(f"Skipping file {ccda_file}: {str(e)}")
                    skipped_files += 1
                    skip_reasons['no_pid'] += 1
                    continue

                encounter_data = parse_ccda(cursor, pid, ccda_path)

                if not encounter_data:
                    logging.warning(f"No valid encounters found in {ccda_file}")
                    skipped_files += 1
                    skip_reasons['no_encounters'] += 1
                    continue

                # Assume the notes file has the same name but with a .txt extension
                notes_file = ccda_file.replace('.xml', '.txt')
                notes_path = os.path.join(notes_folder, notes_file)

                if not os.path.exists(notes_path):
                    logging.warning(f"No corresponding notes file found for {ccda_file}")
                    skipped_files += 1
                    skip_reasons['no_notes'] += 1
                    continue

                notes = parse_clinical_note(notes_path)
                encounters_processed = 0
                for encounter in encounter_data:
                    date = encounter['date']
                    encounter_id = encounter['encounter_id']

                    if date in notes:
                        # Insert into forms table
                        form_id = insert_into_forms(cursor, date, encounter_id, pid)

                        # Insert into form_clinical_notes table
                        insert_into_form_clinical_notes(cursor, form_id, date, pid, encounter_id, notes[date])
                        encounters_processed += 1

                if encounters_processed > 0:
                    processed_files += 1
                else:
                    logging.warning(f"No encounters processed for {ccda_file}")
                    skipped_files += 1
                    skip_reasons['no_encounters'] += 1

                if processed_files % 100 == 0:  # Log progress every 100 files
                    logging.critical(f"Processed {processed_files}/{total_files} files")

            except Exception as e:
                logging.error(f"Error processing file {ccda_file}: {str(e)}")
                skipped_files += 1
                skip_reasons['other_error'] += 1
                continue  # Continue to the next file even if there's an error

    conn.commit()
    cursor.close()
    conn.close()

    logging.critical(f"Processing complete. Total files: {total_files}, Processed: {processed_files}, Skipped: {skipped_files}")
    logging.critical(f"Skip reasons: {skip_reasons}")


# Configuration
db_config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'root',
    'database': 'openemr'
}

ccda_folder = '/home/sunbiz/11k_data/moved_ccda'
notes_folder = '/home/sunbiz/11k_data/moved_notes'

# Run the processing
process_files(ccda_folder, notes_folder, db_config)