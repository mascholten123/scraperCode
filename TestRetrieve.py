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

config_mysql = {
    'user': credentials["user"],
    'password': credentials["password"],
    'host': credentials["host"],
    'port': credentials["port"],
    'database' : credentials["database"],
    'client_flags': [ClientFlag.SSL],
    'ssl_ca': credentials["ssl_ca"]
}

# ----- Query the db --- EXAMPLE -------------------------------------------------
cnx_mysql = mysql.connector.connect(**config_mysql)
cur_mysql = cnx_mysql.cursor(buffered=True)
sql_energyProd = "SELECT * FROM LaReunion_EnergyProd"

cur_mysql.execute(sql_energyProd)
rows = cur_mysql.fetchall()

for row in rows:
    print(row)
