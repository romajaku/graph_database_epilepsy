import pandas as pd
import neo4jConnection as con
import graphParser as gp
import numpy as np
import configparser
import os


# This parser reads cvs files stored in the input directory utilizing the maps stored in the dictionaries directory
# Upon executing the main function a new database will be created

def read_config(path):
    config = configparser.ConfigParser()
    config.read(path)
    uri = config['DATABASE']['URI']
    username = config['DATABASE']['USERNAME']
    password = config['DATABASE']['PASSWORD']
    return uri, username, password


def read_patient_csv(filename):
    df = pd.read_csv(filename)
    df = df.fillna(0)
    df['record_id'] = df['record_id'].astype('int')
    df['gender_other'] = df['gender_other'].astype('int')
    df['dob'] = pd.to_datetime(df['dob'], dayfirst=False)
    df['dod'] = pd.to_datetime(df['dod'], dayfirst=False)
    # df['language_other_ctrl'] = df['language_other_ctrl'].astype('int')
    # df['employment_status_specify_pecclinical'] = df['employment_status_specify_pecclinical'].astype('int')
    # df['social_edu_grade'] = df['social_edu_grade'].astype('int')
    df.replace(0, np.nan, inplace=True)
    df.replace(pd.to_datetime('1970-01-01'), np.nan, inplace=True)
    return df


def read_event_csv(filename):
    # Use a breakpoint in the code line below to debug your script.
    # print(f'Opening file: {filename}')  # Press Ctrl+F8 to toggle the breakpoint.
    df = pd.read_csv(filename)
    df = df.fillna(0)
    df['record_id'] = df['record_id'].astype('int64')
    df['T3_scan_date'] = pd.to_datetime(df['T3_scan_date'], dayfirst=False)
    df['T7_scan_date'] = pd.to_datetime(df['T7_scan_date'], dayfirst=False)
    df['Implant_date'] = pd.to_datetime(df['Implant_date'], dayfirst=False)
    df['Preimplant_MRI_date'] = pd.to_datetime(df['Preimplant_MRI_date'], dayfirst=False)
    df['Preoperative_neuropsych_date'] = pd.to_datetime(df['Preoperative_neuropsych_date'], dayfirst=False)
    df['Surgical_intervention_date'] = pd.to_datetime(df['Surgical_intervention_date'], dayfirst=False)
    df.replace(0, np.nan, inplace=True)
    df.replace(pd.to_datetime('1970-01-01'), np.nan, inplace=True)
    return df


def create_constrains(connection):
    connection.run_query('CREATE CONSTRAINT patients IF NOT EXISTS ON (p:Patient)     ASSERT p.id IS UNIQUE')
    connection.run_query('CREATE CONSTRAINT studies IF NOT EXISTS ON (s:Study)     ASSERT s.name IS UNIQUE')
    connection.run_query('CREATE CONSTRAINT seizures IF NOT EXISTS ON (s:Seizure)     ASSERT s.name IS UNIQUE')
    connection.run_query('CREATE CONSTRAINT lateralizations IF NOT EXISTS ON (l:Lateralization) ASSERT l.name IS UNIQUE')
    connection.run_query('CREATE CONSTRAINT medications IF NOT EXISTS ON (m:Medication)     ASSERT m.name IS UNIQUE')


def create_indexes(connection):
    connection.run_query('CREATE INDEX patient_name_index IF NOT EXISTS FOR (p:Patient) ON (p.id)')
    connection.run_query('CREATE INDEX event_name_index IF NOT EXISTS FOR (e:Event) ON (e.name)')
    connection.run_query('CREATE INDEX study_name_index IF NOT EXISTS FOR (s:Study) ON (s.name)')
    connection.run_query('CREATE INDEX seizure_name_index IF NOT EXISTS FOR (s:Seizure) ON (s.name)')
    connection.run_query('CREATE INDEX later_name_index IF NOT EXISTS FOR (l:Lateralization) ON (l.name)')
    connection.run_query('CREATE INDEX medication_name_index IF NOT EXISTS FOR (m:Medication) ON (m.name)')


def read_all_files(path):
    patient_file = "patient.csv"
    study_file = "protocols.csv"
    event_file = "events.csv"
    summary_file = "summary.csv"
    patient_path = "{}\{}".format(path, patient_file)
    study_path = "{}\{}".format(path, study_file)
    events_path = "{}\{}".format(path, event_file)
    summary_path = "{}\{}".format(path, summary_file)
    patient_df = read_patient_csv(patient_path)
    study_df = pd.read_csv(study_path)
    events_df = read_event_csv(events_path)
    summary_df = pd.read_csv(summary_path)
    return patient_df, study_df, events_df, summary_df


def main():
    uri, username, password = read_config('config.ini')
    db = con.Neo4jConnection(uri, username, password)
    create_constrains(db)
    create_indexes(db)

    path = os.getcwd() + r"\input"
    patient_df, study_df, events_df, summary_df = read_all_files(path)

    parser = gp.GraphParser()
    patient_queries = parser.parse_patients(patient_df)
    db.run_query_list(patient_queries)

    study_queries = parser.create_studies()
    db.run_query_list(study_queries)

    medication_queries = parser.create_medications()
    db.run_query_list(medication_queries)

    study_queries = parser.parse_studies(study_df)
    db.run_query_list(study_queries)

    event_queries = parser.parse_events(events_df)
    db.run_query_list(event_queries)

    epilepsy_queries = parser.create_epilepsy_nodes()
    db.run_query_list(epilepsy_queries)

    epilepsy_connections = parser.parse_epilepsy(summary_df)
    db.run_query_list(epilepsy_connections)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
