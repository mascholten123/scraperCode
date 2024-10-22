#!/home/mirabelle/my-env/bin/python3 -u
# 
# To update etc virtual environment must be activated
# source /home/mirabelle/my-env/bin/activate
#
# Make sure the virtual env. is set as a python interperter - View - Command Pallette - Select Python Interpeter
#
# ---------------------------------------------------------------------------------------
#
# This program is inteded to grab via a JSON object the energy production in La Reunion
# which is recorded ina 15 minute interval.  The data is retrieved and the nadded to a 
# MySQL database for long term querying and projections
#
# ---------------------------------------------------------------------------------------
# load the required libraries
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector.constants import ClientFlag
from sqlalchemy import create_engine, text
import urllib.request
import json
from datetime import datetime
import urllib.request, json 


verbose = 0                 # boolean used for verbose out put - default shoushould be 0
crontab = 1                 # should be set to 1 if runnign as a crontab

if crontab:
    # datetime object containing current date and time
    now = datetime.now()
    # dd/mm/YY H:M:S
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print("-----------------------------------------------------------------------------------------------")
    print("Job Started for laReunionTakamaka at: ", dt_string)

# Lets setup the DB connectors to both  DB´s
# This is the MySQL connection information for the digitalOcean DRwater MySQL DB cluster
# This connection information is stored in the secretes/dbSecrets.json file, which directory
# is EXCLUDED from pushing to gitHub by configuring .gitignore to exclude the secretes/ directory
#
# For the MySQL DB the optional client_flags can not be set via JSON

json_file_path = r"/home/mirabelle/scraperCode/secrets/dbSecrets.json"
with open(json_file_path, "r") as f:
    credentials = json.load(f)

config = {
    'user': credentials["user"],
    'password': credentials["password"],
    'host': credentials["host"],
    'port': credentials["port"],
    'database' : credentials["database"],
    'client_flags': [ClientFlag.SSL],
    'ssl_ca': credentials["ssl_ca"]
}

# test the connection
if verbose:
    print("Connecting to MySQL")

cnx_mysql = mysql.connector.connect(**config)
cur_mysql = cnx_mysql.cursor(buffered=True)
cur_mysql.execute("SHOW STATUS LIKE 'Ssl_cipher'")
if verbose: 
    print(cur_mysql.fetchone())

cur_mysql.close()
cnx_mysql.close()

# Lets get the current DB record count before we add new records to DB
cnx_mysql = mysql.connector.connect(**config)
cur_mysql = cnx_mysql.cursor(buffered=True)
sql_count = "SELECT count(*) FROM LaReunion_Takamaka"
cur_mysql.execute(sql_count)
rows = cur_mysql.fetchone()
rowcount_start = rows[0]
print("Number of records in DB before new extract: ", rowcount_start)

# --- JSON processing ----------------------------------------------------------
try:
    #with urllib.request.urlopen("https://opendata-reunion.edf.fr/api/explore/v2.1/catalog/datasets/prod-electricite-temps-reel/records?limit=100") as url:
    with urllib.request.urlopen("https://opendata-reunion.edf.fr/api/explore/v2.1/catalog/datasets/production_taka_1/records?limit=100") as url:
        data = json.load(url)
        if verbose:
            print(url.status)
            print(data)
except HTTPError as error:
    if verbose:
        print(error.status, error.reason)
    if crontab:
        print(error.status, error.reason)
    exit(0)      
except URLError as error:
    if verbose:
        print(error.reason)
    if crontab:
        print(error.reason)
    exit(0)
except TimeoutError:
    if verbase:
        print("Request timed out")
    if crontab:
        print("Request timed out")
    exit(0)

df = pd.json_normalize(data['results'])
if verbose:
    print(df)

if df.empty:
    if verbose:
	    print("DataFrame is empty!")
    if crontab:
        print("DataFrame is empty! - No new records retrieved!")
        final = datetime.now()
        # dd/mm/YY H:M:S
        dt_string = final.strftime("%d/%m/%Y %H:%M:%S")
        print("Job finished for laReunionTakamaka at: ", dt_string)
    exit(0)

df['time'] = pd.to_datetime(df['time'])
if df['time'].duplicated().any():
    if verbose:
        print("Warning: Duplicate dates found. Using the most recent data for each date.")
    df = df.sort_values('date').drop_duplicates('time', keep='last')

# Create SQLAlchemy engine
engine_string = f"mysql+mysqlconnector://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}?ssl_ca={config['ssl_ca']}"
engine = create_engine(engine_string, connect_args={'ssl_verify_cert': False})

# Store DataFrame in MySQL
table_name = 'LaReunion_Takamaka'
try:
    with engine.begin() as connection:
        # Drop the table if it exists - this should NEVER be rerun after DB is created and data is present
        #connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        
        # Create the table with date as the primary key
        create_table_query = f"""
        CREATE TABLE {table_name} (
            `time` datetime NOT NULL,
            `poste` varchar(64) NOT NULL,
            `valeur` float NOT NULL,
            `debit` float NOT NULL,
            `jour` date NOT NULL,
            `temps_converti` datetime NOT NULL,
            `minutes` int NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        """
        #connection.execute(text(create_table_query))
        
        # Insert data
        #insert_query = f"""
        #INSERT IGNORE INTO {table_name} (date, eolien, hydraulique, photovoltaique, charbon, bioenergies, diesel, stockage, total, jour, statut, heure, turbines_combustion)
        #VALUES (:date, :eolien, :hydraulique, :photovoltaique, :charbon, :bioenergies, :diesel, :stockage, :total, :jour, :statut, :heure, :turbines_combustion)
        #"""
        insert_query = f"""
        INSERT IGNORE INTO {table_name} (time, poste, valeur, debit, jour, temps_converti, minutes)
        VALUES (:time, :poste, :valeur, :debit, :jour, :temps_converti, :minutes)
        """
        connection.execute(text(insert_query), df.to_dict('records'))
    
    if verbose:
        print(f"Data successfully stored in the '{table_name}' table.")
    if crontab:
        print(f"Data successfully stored in the '{table_name}' table.")

except Exception as e:
    if verbose:
        print(f"An error occurred while storing the data: {str(e)}")
    if crontab:
        print(f"An error occurred while storing the data: {str(e)}")

# Verify the data was stored correctly
try:
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        row_count = result.fetchone()[0]
        if verbose:
            print(f"Number of rows in {table_name}: {row_count}")
        if crontab:
            rows_added = row_count - rowcount_start
            print(f"Number of rows added: ", rows_added, " Total Records in DB now: ", row_count)
            # We now need to add finished line
            final = datetime.now()
            # dd/mm/YY H:M:S
            dt_string = final.strftime("%d/%m/%Y %H:%M:%S")
            print("Job finished for laReunionTakamaka at: ", dt_string)

except Exception as e:
    print(f"An error occurred while verifying the data: {str(e)}")

