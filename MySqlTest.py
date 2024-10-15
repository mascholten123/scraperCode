#!/home/mirabelle/my-env/bin/python3 -u
# 
# To update etc virtual environment must be activated
# source /home/mirabelle/my-env/bin/activate
#
# Make sure the virtual env. is set as a python interperter - View - Command Pallette - Select Python Interpeter
#
# ---------------------------------------------------------------------------------------
#
# We are expecting two variables, 
#       argv[1] - is a json object with variable / value pairing
#       argv[2] - is a dummy varibale reserved for later use
#
# ---------------------------------------------------------------------------------------
# load the required libraries
import pandas as pd
import numpy as np
#import sqlite3 as db
import mysql.connector
from mysql.connector.constants import ClientFlag
from sqlalchemy import create_engine, text
import urllib.request
import json
from datetime import datetime

# Lets setup the DB connectors to both  DB´s
# This is the MySQL connection information for the digitalOcean DRwater MySQL DB cluster
# This connection information is stored in the secretes/dbSecrets.json file, which directory
# is EXCLUDED from pushing to gitHub by configuring .gitignore to exclude the secretes/ directory
#
# For the MySQL DB the optional client_flags can not be set via JSON

json_file_path = r"secrets/dbSecrets.json"
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
print("Connecting to MySQL")
cnx_mysql = mysql.connector.connect(**config)
cur_mysql = cnx_mysql.cursor(buffered=True)
cur_mysql.execute("SHOW STATUS LIKE 'Ssl_cipher'")
print(cur_mysql.fetchone())
cur_mysql.close()
cnx_mysql.close()

# ----- Query the db --- EXAMPLE -------------------------------------------------
# cnx_mysql = mysql.connector.connect(**config_mysql)
# cur_mysql = cnx_mysql.cursor(buffered=True)
# sql_weather = "SELECT * FROM table WHERE field= ORDER BY iso_date"

# cur_mysql.execute(sql_weather)
# rows = cur_mysql.fetchall()

#for row in rows:
#    print(row)

# --- JSON processing ----------------------------------------------------------
import urllib.request, json 
with urllib.request.urlopen("https://opendata-reunion.edf.fr/api/explore/v2.1/catalog/datasets/prod-electricite-temps-reel/records?limit=100") as url:
    data = json.load(url)
    print(data)

df = pd.json_normalize(data['results'])
print(df)


df['date'] = pd.to_datetime(df['date'])
if df['date'].duplicated().any():
    print("Warning: Duplicate dates found. Using the most recent data for each date.")
    df = df.sort_values('date').drop_duplicates('date', keep='last')

# Create SQLAlchemy engine
engine_string = f"mysql+mysqlconnector://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}?ssl_ca={config['ssl_ca']}"
engine = create_engine(engine_string, connect_args={'ssl_verify_cert': False})

# Store DataFrame in MySQL
table_name = 'LaReunion_EnergyProd'
try:
    with engine.begin() as connection:
        # Drop the table if it exists
        #connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        
        # Create the table with date as the primary key
        create_table_query = f"""
        CREATE TABLE {table_name} (
            date DATETIME PRIMARY KEY, 
            eolien FLOAT,
            hydraulique FLOAT,
            photovoltaique FLOAT,
            charbon FLOAT,
            bioenergies FLOAT,
            diesel FLOAT,
            stockage FLOAT,
            total FLOAT,
            jour VARCHAR(20),
            statut VARCHAR(20),
            heure VARCHAR(10),
            turbines_combustion FLOAT
        )
        """
        #connection.execute(text(create_table_query))
        
        # Insert data
        insert_query = f"""
        INSERT IGNORE INTO {table_name} (date, eolien, hydraulique, photovoltaique, charbon, bioenergies, diesel, stockage, total, jour, statut, heure, turbines_combustion)
        VALUES (:date, :eolien, :hydraulique, :photovoltaique, :charbon, :bioenergies, :diesel, :stockage, :total, :jour, :statut, :heure, :turbines_combustion)
        """
        connection.execute(text(insert_query), df.to_dict('records'))
    
    print(f"Data successfully stored in the '{table_name}' table.")
except Exception as e:
    print(f"An error occurred while storing the data: {str(e)}")

# Verify the data was stored correctly
try:
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        row_count = result.fetchone()[0]
        print(f"Number of rows in {table_name}: {row_count}")
except Exception as e:
    print(f"An error occurred while verifying the data: {str(e)}")

