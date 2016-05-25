import sqlite3
import MySQLdb as mdb 
import sys

MYSQL_TABLE_NAME = 'snp_scores_2'
TEST_MODE = True 

#TODO: parametrize the number of rows to do... this is currently hardcoded.


"""
A script to import sqlite3 tables into Mysql.
"""
#returns a cursor on the specified mysql db
# mysql_info should be a dict with the following keys:
# sql_host_address, username, password, db_name
def connect_to_mysql(mysql_info):
  connection = mdb.connect(mysql_info['sql_host_address'],
                           mysql_info['username'],
                           mysql_info['password'],
                           mysql_info['db_name'])
  return [connection.cursor(), connection]


#name_of_db should be a valid path to a sqlite3 database
def connect_to_sqlite_db(name_of_db):
  connection = sqlite3.connect(name_of_db)
  return connection.cursor()


#our native-R data has 'chr' as a column name, which is a 
#python reserverd word for the character type. Change it!!!
def get_colnames_for_sqlite_table(sqlite_cursor):
  cursor_desc = sqlite_cursor.description
  headers = [ one_list[0] for one_list in cursor_desc ] 
  headers[headers.index('chr')] = 'chromosome'
  return ", ".join(headers)

#for SQL inserts.
def quote_if_needed(one_value):
  v =  str(one_value)
  if v[0].isalpha():
    return repr(v)
  if len(v) == 1:
    return repr(v)
  return v

#takes a tuple that was returned from 'fetchone' 
def parse_one_sqilte_row_into_list_of_values(fetched_record):
  record_as_list_of_strings = [ quote_if_needed(x) for x in fetched_record ]
  return ", ".join(record_as_list_of_strings)

 
def setup_mysql_connections():
  #this is where I'll hardcode stuff for a while.
  mysql_info = { 'username'         : 'snp_test', 
                 'password'         : 'tester', 
                 'db_name'          : 'motif_score_test_db',
                 'sql_host_address' : '127.0.0.1',
               }
  [mysql_cursor, mysql_connection] =  connect_to_mysql(mysql_info)
  return { 
           'mysql_cursor' : mysql_cursor, 
           'mysql_connection' : mysql_connection,
         }


#sql dict has the keys 'cursor' and 'connection'
def write_output_to_mysql(sql, sql_dict):
  sql_dict['cursor'].execute(sql)
  sql_dict['connection'].commit()  

def write_output_to_file(sql, file_handle):
  file_handle.write(sql)


mysql_info = None
sql_outfile = None

if len(sys.argv) >= 2 and sys.argv[1] == 'test':
  print ("TEST MODE")
  TEST_MODE = True
  sql_outfile = open('sql_out_test_data-2.sql', 'w') 
else:
  mysql_info = setup_mysql_connections()


#get all the sqlite data on one cursor.
sqlite_file='/ua/rhudson/data_in/whole_files/db_1_1.db'
sqlite_table_name = '[db_1_1.Rdata]' 
#is there a way to get this dynamically?

sqlite_cursor = connect_to_sqlite_db(sqlite_file)
sqlite_cursor.execute("SELECT * FROM " + sqlite_table_name )

colnames = get_colnames_for_sqlite_table(sqlite_cursor)


i = 0
one_sqlite_record = sqlite_cursor.fetchone() 
while one_sqlite_record is not None:
  i += 1
  if i % 10000 == 0:
    print("reached "+ str(i) + ' rows')
    break

  one_sqlite_record = sqlite_cursor.fetchone()
  values_list = parse_one_sqilte_row_into_list_of_values(one_sqlite_record)
  sql = "INSERT INTO %(tbl)s  (  %(colnames)s  )  VALUES(  %(values)s )\n" % \
    {'tbl':MYSQL_TABLE_NAME, 'colnames':colnames, 'values':values_list,}
  #print(sql)
  if TEST_MODE:
    sql_outfile.write(sql)
  else:
    mysql_info['mysql_cursor'].execute(sql)
    mysql_info['mysql_connection'].commit()


if TEST_MODE:
  sql_outfile.close() 



