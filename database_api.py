import neo4jConnection as con
import pandas as pd
import configparser


class DatabaseAPI:

    db_connection = None

    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.db_connection = con.Neo4jConnection(self.__uri, self.__user, self.__pwd)

    def find_patient(self, patient_id):
        query = f"MATCH (n:Patient) WHERE n.id={patient_id} RETURN n"
        return self.db_connection.run_query(query)

    def find_patient_by_seizure(self, seizure):
        query = f"MATCH (p:Patient)-[experiences]->(s:Seizure) WHERE s.name=\"{seizure}\" RETURN p"
        return self.db_connection.run_query(query)

    def find_patient_by_study(self, study):
        query = f"MATCH (p:Patient)-[consents]->(s:Study) WHERE s.name=\"{study}\" RETURN p"
        return self.db_connection.run_query(query)

    def find_patient_by_dob(self, dob):
        query = f"MATCH (p:Patient) WHERE p.\"date_of_birth\"=\"{dob}\" RETURN p"
        return self.db_connection.run_query(query)

    def create_patient(self, property_dict):
        query = "CREATE (p: Patient { "
        for key, value in property_dict:
            if isinstance(value, int):
                query += f"{key}: {value}, "
            elif isinstance(value, pd.Timestamp):
                query += f"{key}: date(\'{value.date()}\'), "
            else:
                query += f"{key}: \"{value}\", "
        query += "}) RETURN p"
        return self.db_connection.run_query(query)

    def create_indexes(self):
        self.db_connection.run_query('CREATE INDEX patient_name_index IF NOT EXISTS FOR (p:Patient) ON (p.id)')
        self.db_connection.run_query('CREATE INDEX event_name_index IF NOT EXISTS FOR (e:Event) ON (e.name)')
        self.db_connection.run_query('CREATE INDEX study_name_index IF NOT EXISTS FOR (s:Study) ON (s.name)')
        self.db_connection.run_query('CREATE INDEX seizure_name_index IF NOT EXISTS FOR (s:Seizure) ON (s.name)')
        self.db_connection.run_query('CREATE INDEX later_name_index IF NOT EXISTS FOR (l:Lateralization) ON (l.name)')
        self.db_connection.run_query('CREATE INDEX medication_name_index IF NOT EXISTS FOR (m:Medication) ON (m.name)')

    def create_constrains(self):
        self.db_connection.run_query('CREATE CONSTRAINT patients IF NOT EXISTS ON (p:Patient)     ASSERT p.id IS UNIQUE')
        self.db_connection.run_query('CREATE CONSTRAINT studies IF NOT EXISTS ON (s:Study)     ASSERT s.name IS UNIQUE')
        self.db_connection.run_query('CREATE CONSTRAINT seizures IF NOT EXISTS ON (s:Seizure)     ASSERT s.name IS UNIQUE')
        self.db_connection.run_query(
            'CREATE CONSTRAINT lateralizations IF NOT EXISTS ON (l:Lateralization) ASSERT l.name IS UNIQUE')
        self.db_connection.run_query(
            'CREATE CONSTRAINT medications IF NOT EXISTS ON (m:Medication)     ASSERT m.name IS UNIQUE')
