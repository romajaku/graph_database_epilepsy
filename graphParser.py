import pandas as pd
import csv
import numpy as np


class GraphParser:
    patient_property_dict = None
    study_protocol_dict = None
    seizure_types_dict = None
    lateralization = [
        "Left only", "Right only", "Inconclusive",
        "Bilateral independent Seizure Onsets", "Right > Left", "Left > Right"
    ]
    medication_list = [
        'Lamotrigine', 'Levetiracetam', 'Lorazepam', 'Clonazepam', 'Depakote', 'Oxcarbazepine', 'Topiramate',
        'Clobazam', 'Zonisamide', 'Carbamazepine', 'Perampanel', 'Lacosamide', 'Phenobarbital', 'Phenytoin',
        'Eslicarbazepine acetate', 'Pregabalin', 'Primidone', 'Cenobamate'
    ]

    def __init__(self):
        with open('dictionaries/patient_property_map.csv') as infile:
            reader = csv.reader(infile)
            self.patient_property_dict = dict((rows[0], rows[1]) for rows in reader)
        with open('dictionaries/study_protocol_map.csv') as infile:
            reader = csv.reader(infile)
            self.study_protocol_dict = dict((rows[0], rows[1]) for rows in reader)
        with open('dictionaries/epilepsy_type_map.csv') as infile:
            reader = csv.reader(infile)
            self.seizure_types_dict = dict((rows[0], rows[1]) for rows in reader)

    def parse_patients(self, df):
        queries = []
        for index, row in df.iterrows():
            query = ""
            if row['record_id']:
                query += "MERGE (p:Patient { "
            else:
                continue
            for key, value in row.items():
                if value is not np.nan and value is not pd.NaT and key in self.patient_property_dict:
                    if key == "record_id":
                        query += self.patient_property_dict.get(key) + f": {value}, "
                    elif isinstance(value, pd.Timestamp):
                        query += self.patient_property_dict.get(key) + f": date(\'{value.date()}\'), "
                    else:
                        query += self.patient_property_dict.get(key) + f": \"{value}\", "
            query = query[:-2]
            query += " })-[:diagnosed]->(d:Diagnosis {name: \"Summary Clinical History\"})"
            queries.append(query)
        return queries

    def parse_events(self, df):
        dates_df = df.drop(['record_id', 'T3_subject_id', 'T7_subject_id', 'Surgical_intervention'], axis=1)
        queries = []
        for index, row in dates_df.iterrows():
            patient = str(int(df.loc[index].at["record_id"]))
            row.sort_values(inplace=True)
            row.dropna(inplace=True)
            if not row.empty:
                query = f"MATCH (p:Patient) WHERE p.id = {patient} "
                i = 1
                for event, date in row.items():
                    if i == 1:
                        event_name = event.removesuffix('_date').replace("_", " ")
                        query += "CREATE (e" + str(i) + ":Event { name:\"" + event_name + "\", "
                        query += f"date: date(\'{date.date()}\')" + "}), "
                        query += f"(p) -[:next]-> (e{i}), "
                    else:
                        event_name = event.removesuffix('_date').replace("_", " ")
                        query += "(e" + str(i) + ":Event { name:\"" + event_name + "\", "
                        query += f"date: date(\'{date.date()}\')" + "}), "
                        query += f"(e{i-1}) -[:next]-> (e{i}), "
                    i += 1
                query += f"(p)-[:last]->(e{i - 1})"
                queries.append(query)
        return queries

    def parse_studies(self, df):
        queries = []
        for index, row in df.iterrows():
            for key, value in self.study_protocol_dict.items():
                if key in row:
                    if row[key] == "Checked":
                        query = "MATCH (p:Patient {id: " + str(row['record_id']) + "}), "
                        query += "(s:Study {name: \"" + value + "\"}) "
                        query += "MERGE (p) -[r:consents]-> (s)"
                        # query += "SET r" + key + ".date = \"" + str(row[key + "_date"]) + "\" "
                        queries.append(query)
        return queries

    def parse_epilepsy(self, df):
        queries = []
        for index, row in df.iterrows():
            for key, value in self.seizure_types_dict.items():
                if key in row:
                    if row[key] == 1:
                        query = "MATCH (p:Patient {id: " + str(int(row['record_id'])) + "})-[:diagnosed]->(d:Diagnosis), "
                        query += "(s:Seizure {name: \"" + value + "\"}) "
                        query += "MERGE (d) -[r:experiences]-> (s)"
                        queries.append(query)
            if row['emu_seizure_lateralization_pecclinical'] is not np.nan:
                query = "MATCH (p:Patient {id: " + str(int(row['record_id'])) + "})-[:diagnosed]->(d:Diagnosis), "
                query += "(l:Lateralization {name: \"" + row['emu_seizure_lateralization_pecclinical'] + "\"}) "
                query += "MERGE (d) -[r:localized]-> (l)"
                queries.append(query)
            if row['medication'] is not np.nan:
                medications = [x.strip() for x in row['medication'].split(',')]
                for medication in medications:
                    if medication in self.medication_list:
                        query = "MATCH (p:Patient {id: " + str(
                            int(row['record_id'])) + "})-[:diagnosed]->(d:Diagnosis), "
                        query += "(m:Medication {name: \"" + medication + "\"}) "
                        query += "MERGE (d) -[r:takes]-> (m) "
                queries.append(query)
        return queries

    def create_studies(self):
        queries = []
        for study in self.study_protocol_dict.values():
            query = "MERGE (s:Study {name: \"" + study + "\"})"
            queries.append(query)
        return queries

    def create_medications(self):
        queries = []
        for med in self.medication_list:
            query = "MERGE (s:Medication {name: \"" + med + "\"})"
            queries.append(query)
        return queries

    def create_epilepsy_nodes(self):
        queries = []
        for seizure in self.seizure_types_dict.values():
            query = "MERGE (s:Seizure {name: \"" + seizure + "\"})"
            queries.append(query)
        for later in self.lateralization:
            queries.append("MERGE (l:Lateralization {name: \"" + later + "\"})")
        return queries

