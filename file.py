import os
import pyodbc
import configparser
from datetime import datetime

#read config
parser = configparser.ConfigParser()
parser.read("file.conf")
server = parser.get("database_config", "server")
database = parser.get("database_config", "database")
username = parser.get("database_config", "username")
password = parser.get("database_config", "password")
driver = parser.get("database_config", "driver")
drivedir = parser.get("file_config", "drivedir")
tablename = parser.get("database_config","stagingtable")
conn_str = 'DRIVER={0};SERVER={1};DATABASE={2};UID={3};PWD={4}'
conn_str = conn_str.format(driver,server,database,username,password)

def pull_files_info(search_path):
  result =[]
  #Walking top-down from the root
  for root, dir, files in os.walk(search_path):
    for file in files:
      result.append((os.path.join(root, file), root, file, os.path.getsize(os.path.join(root, file))))
    
  print("File List Size:", len(result) )
  result = list(set([i for i in result]))
  print("Cleaned List Size:", len(result) )
  return result
  
def createstagingtable(cursor, stagingtablename):
  drop = 'DROP TABLE IF EXISTS {0}'.format(stagingtablename)
  create = 'CREATE TABLE {0} (fid bigint IDENTITY NOT NULL PRIMARY KEY, fpath varchar(max), ffolder varchar(max), fname varchar(max), fsize bigint)'.format(stagingtablename)
  sql = '{0};{1};'.format(drop,create)
  #index = 'CREATE INDEX {0}_id ON {0}(file_id)'.format(stagingtablename)
  #sql = '{0};{1};{2};'.format(drop,create,index)
  cursor.execute(sql)

def insertmanytostaging(cursor, stagingtablename, records):
  sql = 'INSERT INTO {0} (fpath, ffolder, fname, fsize) VALUES (?, ?, ?, ?)'.format(stagingtablename)
  cursor.fast_executemany = True
  cursor.executemany(sql, records)
  print(f'{len(records)} rows inserted to the {stagingtablename} table')  

# datetime object containing current date and time
now = datetime.now()
print("Pull Files Start =", now)
files = pull_files_info(drivedir)
now = datetime.now()
print("Pull Files End =", now)

conn = pyodbc.connect(conn_str)
if conn is None:
  print("Error connecting to the database")
  print("Aborting database logic")
else:
  print("Connection established!")
  conn.autocommit = True
  cursor = conn.cursor()  
  createstagingtable(cursor, tablename)
  now = datetime.now()
  print("Insert Records Start =", now)
  insertmanytostaging(cursor, tablename, files)
  now = datetime.now()
  print("Insert Records End =", now)
  cursor.close()
  conn.close()

