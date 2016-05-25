import sqlite3
import MySQLdb as mdb 
"""
A script to import sqlite3 tables into Mysql.
"""
#This should be DRYd up....


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

def get_colnames_for_sqlite_table(sqlite_cursor):
  cursor_desc = sqlite_cursor.description
  headers = [ one_list[0] for one_list in cursor_desc ] 
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


#We are not going to be outputting to an actual SQL table, but instead to  
#a text file full of INSERTs.
#this is where I'll hardcode stuff for a while.
#mysql_info = { 'username'         : 'snp_test', 
#               'password'         : 'tester', 
#               'db_name'          : 'motif_score_test_db',
#               'sql_host_address' : '127.0.0.1',
#             }
#mysql_table_name = 'snp_scores_1'
#[mysql_cursor, mysql_connection] =  connect_to_mysql(mysql_info)
#
sql_outfile = open('sql_out_test_data-0', 'w')


#get all the sqlite data on one cursor.
sqlite_file = '/ua/rhudson/data_in/one_hg_scores.db'
sqlite_table_name = 'motifscores_hg38_1_14'
sqlite_cursor = connect_to_sqlite_db(sqlite_file)
sqlite_cursor.execute("SELECT * FROM " + sqlite_table_name )

colnames = get_colnames_for_sqlite_table(sqlite_cursor)

i = 0
one_sqlite_record = sqlite_cursor.fetchone() 
while one_sqlite_record is not None:
  one_sqlite_record = sqlite_cursor.fetchone()
  values_list = parse_one_sqilte_row_into_list_of_values(one_sqlite_record)
  sql = "INSERT INTO %(tbl)s  (  %(colnames)s  )  VALUES(  %(values)s )   " % \
    {'tbl':mysql_table_name, 'colnames':colnames, 'values':values_list,}
  print(sql)
  sql_outfile.write(sql)


sql_outfile.close() 
print "DONE!"


