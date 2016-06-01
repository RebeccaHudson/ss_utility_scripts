import sqlite3
import MySQLdb as mdb 
from cassandra.cluster import Cluster
import sys

CASSANDRA_TABLE_NAME = 'snp_scores_5'


#TODO: parametrize the number of rows to do... this is currently hardcoded.

"""
A script to import sqlite3 tables into Cassandra.
"""

#leave it hardcoded for now...
def connect_to_cassandra():
  cluster = Cluster(['fugu'])  
  session = cluster.connect('rsnp_data') #specify a keyspace
  return session  #say session.execute( cql )

#name_of_db should be a valid path to a sqlite3 database
def connect_to_sqlite_db(name_of_db):
  connection = sqlite3.connect(name_of_db)
  return connection.cursor()

def get_colnames_for_sqlite_table(sqlite_cursor):
  cursor_desc = sqlite_cursor.description
  headers = [ one_list[0] for one_list in cursor_desc ] 
  return ", ".join(headers)


def integerify_if_needed(one_value):
  pass  


#for SQL inserts, will quote anything that starts with a letter,
#also will quote + and -
def quote_if_needed(one_value):
  v =  str(one_value)
  if v[0].isalpha():
    return repr(v)
  if len(v) == 1:
    return repr(v)
  return v

#coerce floats that are really ints into being ints.
def process_value(one_value):
  #print("before: type of value" + str(type(one_value)) + " value: " + str(one_value))
  if type(one_value) == float and one_value.is_integer():
    one_value = int(one_value)
  if type(one_value) is not float and type(one_value) is not int:
    one_value = quote_if_needed(one_value)
  #print("after: type of value" + str(type(one_value)) + " value: " + str(one_value))
  return one_value 

#takes a tuple that was returned from 'fetchone' 
def parse_one_sqilte_row_into_list_of_values(fetched_record):
  record_as_list_of_strings = [ str(process_value(x)) for x in fetched_record ]
  return ", ".join(record_as_list_of_strings)

#TODO fix cassandra driver for python.. 
#c_session = connect_to_cassandra()

#get all the sqlite data on one cursor.
sqlite_input_dir='/ua/rhudson/data_in/whole_files/'
sqlite_file_name='db_1_1.db'
sqlite_file = sqlite_input_dir + sqlite_file_name
sqlite_table_name = '[db_1_1.RData]'

sqlite_cursor = connect_to_sqlite_db(sqlite_file)
sqlite_cursor.execute("SELECT * FROM " + sqlite_table_name )

colnames = get_colnames_for_sqlite_table(sqlite_cursor)

i = 0
one_sqlite_record = sqlite_cursor.fetchone() 
while one_sqlite_record is not None:
  i += 1

  one_sqlite_record = sqlite_cursor.fetchone()
  values_list = parse_one_sqilte_row_into_list_of_values(one_sqlite_record)
  sql = "INSERT INTO %(tbl)s  (  %(colnames)s  )  VALUES(  %(values)s );" % \
    {'tbl':CASSANDRA_TABLE_NAME, 'colnames':colnames, 'values':values_list,}
  print sql
  if i % 1000 == 0:
    #print("reached "+ str(i) + ' rows')
    #print sql
    break 
  #c_session.execute(sql)

