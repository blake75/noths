import os, inspect
from load_data import csv_path, load_nypd_data
import sqlite3
from sqlite3 import Error

this_file_path=os.path.abspath(inspect.getsourcefile(lambda:0)) #location of this file
outputs_path=os.path.join(this_file_path, "..", "..", "outputs") 
outputs_path=os.path.normpath(outputs_path)

nypd_list=load_nypd_data(csv_path)

def requirement_4(nypd_list, db_file=''):
    print("\nFulfilling Requirement 4\n")    
    if not db_file: db_file=os.path.join(outputs_path, 'pythonsqllite.db')
    log=''
    cols=", ".join(nypd_list[0])
    vals=str("?,"*len(nypd_list[0]))[:-1]
    table_name="nypd_data"
    conn = sqlite3.connect(db_file)
    cur=conn.cursor()
    try:
        create_sql=f"CREATE TABLE {table_name} ({cols});"
        cur.execute(create_sql)
        insert_sql=f"INSERT INTO {table_name} ({cols}) VALUES ({vals});"
        cur.executemany(insert_sql, nypd_list[1:])
        conn.commit()
        log="\nSQLliteDatabase created. Ready to accept sql. Try querying the [nypd_data] table."
        print(log)
    except Error as e:
        print(e)
    finally:
        conn.close()
        return log

def test_database(sql):
    results=[]
    db_file=os.path.join(outputs_path, 'pythonsqllite.db')
    conn = sqlite3.connect(db_file)
    cur=conn.cursor()
    try:
        cur.execute(sql)
        results=cur.fetchall()
    except Error as e:
        print(e)
    finally:
        conn.close()
    return results

requirement_4(nypd_list)
test_results=test_database("select * from nypd_data limit 10;")
print(test_results)