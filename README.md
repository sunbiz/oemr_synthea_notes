# Importing synthea-generated notes to OpenEMR
By default synthea will produce CCDA xml and notes separately in the output folder, along with FHIR or additional data
types that you will have requested. This repository contains Python scripts that will allow you to import the clinical
notes into the appropriate OpenEMR tables - `form_clinical_notes` and `forms`.

## Selected patient records that you need
The `move_patient_ccda_notes/py` script will move from synthea output folder to another folder to subset the data you
want to import.

## Import ccda and notes by matching into OpenEMR database
The `import_clinical_notes.py` will import into the notes with `'default'` and `admin (id=1)` user into the 
`form_clinical_notes` table. It will add a reference row into the `forms` table with the `formdir='clinical_notes'` and 
`form_type='Clinical Notes'`. You should customize these defaults, if you have a different implementation in your EMR.

## Troubleshooting (Queries to fix import issues)
* Performance issues. too many useless drug_units were existing. Had to drop them from the database:
```
delete from list_options where list_id='drug_units' and title = '';
```

* For OpenEMR v7.0.2, these are important to correctly show the encounters:
```
ALTER TABLE `form_encounter` ADD `ordering_provider_id` INT(11) DEFAULT '0';
ALTER TABLE `form_encounter` ADD `last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
```

* Once the form_clinical_notes and forms tables are moved, to add reference entries:
```
CREATE INDEX idx_forms_id ON forms (id);
CREATE INDEX idx_forms_form_id ON forms (form_id);
CREATE INDEX idx_forms_formdir ON forms (formdir);
 
CREATE INDEX idx_form_clinical_notes_id ON form_clinical_notes (id);
CREATE INDEX idx_form_clinical_notes_form_id ON form_clinical_notes (form_id);
CREATE INDEX idx_form_clinical_notes_pid ON form_clinical_notes (pid);
 
UPDATE openemr.forms SET form_id = id WHERE formdir = 'clinical_notes';
```

* Unrelated, by useful to get random SSN-like in patient full
```

CREATE TEMPORARY TABLE temp_patient_numbers AS
SELECT
    id,
    @row := @row + 1 AS row_num
FROM
    patient_data,
    (SELECT @row := 0) AS init
ORDER BY
    RAND();

UPDATE patient_data p
JOIN temp_patient_numbers t ON p.id = t.id
SET p.ss = CONCAT(
    LPAD(1 + MOD(t.row_num * 17 + 123, 900), 3, '0'),
    '-',
    LPAD(1 + MOD(t.row_num * 31 + 456, 100), 2, '0'),
    '-',
    LPAD(1 + MOD(t.row_num * 73 + 789, 10000), 4, '0')
);

DROP TEMPORARY TABLE temp_patient_numbers;

```
