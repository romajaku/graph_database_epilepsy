# graph_database_epilepsy

## Prerequisites

### Libraries

To install all the necessary libraries (listed in) please execute the command:

``
pip install -r requirements.txt
``

### Database

Ensure that an instance of the Neo4j graph database is running and available on the localhost server.
Check the config.ini file and update it with the correct bolt port and address
for the database as well as the credentials. Default credentials provided as example.

### Input files

Ensure that the appropriate input files are placed in the input folder.
The patient.csv file contains fields pertaining to the patient, the protocols.csv file contains information
about the studies each patient has consented to, the events includes information about the procedures performed
on a patient and the summary.csv file consists of the diagnosis established during the surgical conference.

## Initializing Database

To initialize the database with data simply execute the main function of the main.py file using the command:

``
$ python main.py
``
