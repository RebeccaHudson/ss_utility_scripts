import sqlite3
#from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch
import json
import sys
import requests

#TODO: parametrize the number of rows to do... this is currently hardcoded.

"""
A script to import sqlite3 tables into Elasticsearch
"""

#name_of_db should be a valid path to a sqlite3 database
def connect_to_sqlite_db(name_of_db):
  connection = sqlite3.connect(name_of_db)
  return connection.cursor()

def get_colnames_for_sqlite_table(sqlite_cursor):
  cursor_desc = sqlite_cursor.description
  headers = [ one_list[0] for one_list in cursor_desc ] 
  return headers

#takes a tuple that was returned from 'fetchone' 
def parse_one_sqilte_row_into_json_data(colnames, fetched_record):
    #this might be very close to what we need already. 
    print colnames
    print fetched_record  
    happy = zip(colnames, fetched_record)
    happy = dict(happy)
    return happy


def put_json_into_elasticsearch(es, json):
     #r = requests.post('https://quasar-19/atsnp_data/scores_output', data=json)    
     res = es.index(index="atsnp_data", doc_type="atsnp_output", body=json)
     #print res['created'] 
     #this gets everything, and prints it into the browser 
     #http://quasar-18:9200/atsnp_data/_search?q=*
 
#TODO fix cassandra driver for python.. 
#c_session = connect_to_cassandra()

def get_one_bulk_action_json(json_record):
    bulkj = {
    '_index': 'atsnp_data',
    '_type' : 'atsnp_output',
    '_source': { json_record }
    }
    return bulkj


def put_bulk_json_into_elasticsearch(es, action):
    json = json.dumps(action)
    result = es.bulk(json)
    return result

#get all the sqlite data on one cursor.
sqlite_input_dir='/ua/rhudson/data_in/whole_files/'
sqlite_file_name='db_1_1.db'
sqlite_file = sqlite_input_dir + sqlite_file_name
sqlite_table_name = '[db_1_1.RData]'

sqlite_cursor = connect_to_sqlite_db(sqlite_file)
sqlite_cursor.execute("SELECT * FROM " + sqlite_table_name )

colnames = get_colnames_for_sqlite_table(sqlite_cursor)


es = Elasticsearch('quasar-19')
es_chunk_size = 200 

i = 0
action = []
one_sqlite_record = sqlite_cursor.fetchone() 
while one_sqlite_record is not None:
  i += 1
  one_sqlite_record = sqlite_cursor.fetchone()
  json_data = parse_one_sqilte_row_into_json_data(colnames, one_sqlite_record)
  one_bulk_action_json = get_one_bulk_action_json(json_data)
  action.append(json_data)
  #make an iterable to put into elasticsearch
  if i % es_chunk_size  == 0:
      print "reached "+ str(i) + ' rows'
      result = put_bulk_json_into_elasticsearch(es, action)
      print "result from bulk put " + str(result)
  #put_json_into_elasticsearch(es, json_data)









    
