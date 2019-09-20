#This is the database management file for the fake_brexit app
#This collects together SQL queries and the sqlite DB so all the connections are done in one place
# The serious analysis will happen in pandas dataframes, so 
#from os import listdir
import sys
import sqlite3
from sqlite3 import Error
import csv
import pandas as pd

# Twitter user and status objects are quite detailed. See the dev.twitter.com for documentation.
# tweepy seems to have stuck to twitter naming conventions.
# Comment each feature FTW and note that SQL Comment Syntax needs to be used for SQL code, rather than python.

debug = False #True #

#Twitter user object
# The id is unique, so is a primary key.
#
user = '''CREATE TABLE IF NOT EXISTS user (
default_profile	 INTEGER default 0,
id	         INTEGER(6) NOT NULL,
id_str	         TEXT default '',
retrieved        TEXT default '',
screen_name	 TEXT NOT NULL,
profile_background_tile	 INTEGER default 0,
following	 INTEGER default 0,
description	 TEXT default '',
name	         TEXT default '',
profile_sidebar_border_color	 TEXT default '',
utc_offset	 TEXT default '',
statuses_count	 INTEGER default 0,
notifications	 INTEGER default 0,
verified	 INTEGER default 0,
profile_background_image_url_https	 TEXT default '',
profile_image_url	 TEXT default '',
default_profile_image	 INTEGER default 0,
profile_image_url_https	 TEXT default '',
geo_enabled	 INTEGER default 0,
follow_request_sent	 INTEGER default 0,
is_translation_enabled	 INTEGER default 0,
profile_use_background_image	 INTEGER default 0,
protected	 INTEGER default 0,
favourites_count INTEGER default 0,
url	 TEXT default '',
followers_count	 INTEGER default 0,
profile_background_image_url	 TEXT default '',
profile_link_color	 TEXT default '',
profile_text_color	 TEXT default '',
profile_banner_url	 TEXT default '',
has_extended_profile	 INTEGER default 0,
is_translator	 INTEGER default 0,
profile_sidebar_fill_color	 TEXT default '',
created_at	 TEXT default '',
contributors_enabled	 INTEGER default 0,
friends_count	 INTEGER default 0,
profile_background_color	 TEXT default '',
location	 TEXT default '',
time_zone	 TEXT default '',
listed_count	 INTEGER default 0,
lang	 TEXT default '',
PRIMARY KEY (id));'''

#-- Status is a 'tweet'
# The id is unique, so is a primary key.
#Each tweet is made by a user, so the user needs to be created first
#(Conversely drop the status table before the user to avoid foreign key errors.
#
status = '''CREATE TABLE IF NOT EXISTS status (
id	INTEGER INTEGER NOT NULL,
id_str	TEXT default '',
created_at	TEXT default '',
timestamp_ms    INTEGER(6) default 0,
text	TEXT NOT NULL,
in_reply_to_status_id	INTEGER default 0,
entities TEXT default '',
possibly_sensitive	INTEGER default 0,
in_reply_to_user_id_str	TEXT default '',
coordinates	TEXT default '',
retweeted	INTEGER default 0,
retweet_count	INTEGER default 0,
retweeted_status INTEGER default 0,
quote_count INTEGER default 0,
quoted_status INTEGER default 0,
contributors	TEXT default '',
favorite_count	INTEGER default 0,
favorited	INTEGER default 0,
in_reply_to_status_id_str	TEXT default '',
source	TEXT default '',
in_reply_to_user_id	INTEGER default 0,
user	INTEGER(6) not null,
place	TEXT default '',
geo	TEXT default '',
truncated	INTEGER default 0,
in_reply_to_screen_name	TEXT default '',
is_quote_status	INTEGER default 0,
lang	TEXT default '',
PRIMARY KEY (id),
FOREIGN KEY (user) REFERENCES user (id) 
            ON DELETE CASCADE ON UPDATE NO ACTION);'''

#Quotes in strings upset the sqlite query generation, so convert these to \'
#
def get_str(item):
  val = '"None"'
  if isinstance(item,int):
    val="%d" % (item)
  elif isinstance(item,str):
    descr = item.replace('"', r'\'')
    val = '"' + format(descr) + '"'
  else:
      val='"%s"' % (item)
      if debug:
          print("get_str type: {}".format(type(item)))
  return val

class Db:
    """Wrapper Class for the SQLite Database"""
 #   table_name = 'status'
    csv_name = "status_derived"
    df = None                   # pandas dataframe from DB
    
    def __init__(self, db_file):
    '''cache the connection object for reuse'''
        try:
            conn = sqlite3.connect(str(db_file))
            print('SQLite Version OK', sqlite3.version)
            self.conn = conn
        except Error as e:
            print("Fake_DB DB file", db_file, ": ", e)

    def initialize(self):
        """(Re) Initialize the status table"""
        drop = "DROP TABLE IF EXISTS status;"
        drop2 = "DROP TABLE IF EXISTS user;"
        try:
            self.conn.execute(drop)
            self.conn.execute(drop2)
            self.conn.execute(user)
            self.conn.execute(status)
            self.conn.commit()
        except Error as e:
            print("Fake_DB initialize drops: {}".format(e))

#create the user and status entries unless they exist. Just include the essential data, then
    def add_userStatusID(self, user_dict, status_dict):
        try:
            uid = user_dict['id']
            cursor = self.conn.cursor()
            cursor.execute('''SELECT * FROM user WHERE (id=?)''', (uid,))  # user present?
            entry = cursor.fetchone()
            if entry is None: #no user with this ID, so insert
                cursor.execute('''INSERT OR REPLACE INTO user(id, screen_name, retrieved) VALUES(?,?,?)''',(user_dict['id'], user_dict['screen_name'], user_dict['retrieved']))
            cursor.execute('''INSERT OR REPLACE INTO status(id, user, text) VALUES(?,?,?)''',(status_dict['id'], user_dict['id'], status_dict['text']))
 #           self.conn.commit()
        except sqlite3.Error as e:
            self.logerror("add_userStatusID: Database error: %s" % e)
        except Exception as e:
            self.logerror("add_userStatusID: Exception in _query: %s" % e)
        return True

      
    def committ(self):
        '''Better transaction? by committing once per tweet. Encapsulate commit rather than using commit and DB connects everywhere'''
        try:
 #           cursor = self.conn.cursor()
            self.conn.commit()
        except sqlite3.Error as e:
            self.logerror("committ: Database error: %s" % e)
        except Exception as e:
            self.logerror("committ: Exception in _query: %s" % e)
        return True  

#Pass the statistics in a dict, so the number of stats can vary. 
#This is neither secure or efficient, so this code is for research apps only, not production
#
    def addAllData(self, id, TS_dict):
#        print("TS_dict", TS_dict)
        cols = [key + "=" + get_str(value) + "," for (key, value) in TS_dict.items()]
        if 'text' in TS_dict:           # its a status
            sql = 'UPDATE status SET ' + ' '.join(map(str, cols))
        elif 'default_profile' in TS_dict: 
            sql = 'UPDATE user SET ' + ' '.join(map(str, cols))
        else:
            print("addAllData:: unrecognized dict {}".format(TS_dict))
            sql = "-- --"
            id = 0
        sql2 = sql[:-1] + ' WHERE id=?'
        if debug:                   #lots of output
            print("addAllData::Query: ", sql2)
 #       return True
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql2, (id,))
 #           self.conn.commit()
        except sqlite3.Error as e:
            print("Error: addAllData::Query: ", sql2)
            self.logerror("addAllData: Database error: %s" % e)
        except Exception as e:
            self.logerror("addAllData: Exception in _query: %s" % e)
        return True

    # to avoid reprocessing data in db. If recalc is really needed, zero db.
    #
    def check_processed(self, filename, column):
        sql = '''SELECT {} from status where Filename=?;'''.format(column)
#        print("check_unprocessed: SQL:\n{}".format(sql))
        cur = self.conn.cursor()
        Found = False
        try:
            cur.execute(sql, (filename,))
            result = cur.fetchone()
        except Exception as e:
            self.logerror("check_processed: Exception in 0_query: %s" % e)
        if result and (result[0] > 0):
            print("Skipping  Filename {} as {} in DB".format(filename, column))
            Found = True
        else:
            print("Column {} for Filename {} is not in DB".format(column, filename))
            Found = False
        return Found
    
    
    def logerror(self, msg):
        print("Error (DB): ", msg)

    def index_filenames(self):
        cur = self.conn.cursor()
        try:
            cur.execute("DROP INDEX tag_titles;")
            cur.execute("CREATE INDEX tag_titles ON status(Filename);")
            self.conn.commit()
        except Exception as e:
            self.logerror("index_filenames: Exception in _query: %s" % e)         
        

    def ShowTables(self):
        cur = self.conn.cursor() 
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        available_table=(cur.fetchall())
        [self.ShowStatus(table[0]) for table in available_table]
    #    print("Tables:", available_table)

#Helpful page: https://sebastianraschka.com/Articles/2014_sqlite_in_python_tutorial.html
        
    def ShowStatus(self, table):
        cur = self.conn.cursor() 
        dataCopy = cur.execute("SELECT COUNT(*) FROM " + table)
        values = dataCopy.fetchone()
        print("Table: ", table, " Entries: ", values[0])
    #    print(f'DB File: {dbfile:s}\n');
    #    print(f"{0} Entries in {1}\n" . format(values, table))

    def values_in_col(self, table_name):
        """ Returns a dictionary with columns as keys
        and the number of not-null entries as associated values.
        """
        cursor = self.conn.cursor() 
        cursor.execute('PRAGMA TABLE_INFO({})'.format(table_name))
        info = cursor.fetchall()
        col_dict = dict()
        for col in info:
            col_dict[col[1]] = 0
        try:
            for col in col_dict:
                cursor.execute('''SELECT (?) FROM ? WHERE ? IS NOT NULL''', (col, table_name, col))
                col_dict[col] = len(cursor.fetchall())
        except Exception as e:
            self.logerror("values_in_col: Exception in _query: %s" % e)         
        print("\nNumber of entries per column in table: {}" . format(table_name))
        for i in col_dict.items():
            print('{}: {}'.format(i[0], i[1]))
            return col_dict

    def getCols(self, table):
        cursor = self.conn.cursor()
        cursor.execute('PRAGMA TABLE_INFO({})'.format(table))
        info = [cols[1] for cols in  cursor.fetchall()]
        return(info)

    def SaveCSV(self, table):    
        cursor = self.conn.cursor() 
        cursor.execute(f"SELECT * FROM {table}")
        cols = self.getCols(table)
        print("Cols:", cols)
        with open(self.csv_name + '_' + table + ".csv", "w", newline='') as csv_file:  # Python 3 version    
            csv_writer = csv.writer(csv_file, dialect='excel')
            csv_writer.writerow([i[0] for i in cursor.description]) # write headers
            csv_writer.writerows(cursor)

#Get the pandas Dataframe
    def getDF(self, table, cols = "*", get_again = False):
#        if self.df.empty() or get_again:
#        cursor = self.conn.cursor() 
        sqlqry  = f"SELECT {cols} FROM {table}"
        self.df = pd.read_sql_query(sqlqry, self.conn)
        return self.df

    def FindLoadedIDsUser(self, table = 'user'):
        cursor = self.conn.cursor() 
        cursor.execute("SELECT DISTINCT ID FROM user")
        return [element[0] for element in cursor.fetchall()]
    
            
    def db_close(self):
        self.conn.close()

#This block runs if the file is executed directly, rather than included.
#CAUTION: This will zero the database.
# (Useful in dev)
if __name__ == '__main__':
    try:
        DB = Db("fake_db.sqlite")
        DB.initialize()
    except Exception as e:
        print("__name__ : Exception in Fake_DB main {}".format(e))

        # DB.ShowTables()
#         DB.values_in_col('status')
#         DB.values_in_col('user')
#         DB.getCols('status')
# #        DB.SaveCSV('status')
# #        DB.SaveCSV('user')
# #    args = sys.argv[1:]
# #    if args[0] == "":
# #        resetting = False  # False # True
# #    else:
# #        resetting = True  # False # True
# #    print("Arg: {}, Reset: {}".format(sys.argv[1:], resetting))
# #    if resetting:
#     DB.initialize()
#  #   else:
#  #       DB.index_filenames()
#     DB.ShowStatus('status')
#     DB.ShowStatus('user')
#     print(DB.FindLoadedIDsUser('user'))
# #    DB.df.describe()
#     DB.db_close()