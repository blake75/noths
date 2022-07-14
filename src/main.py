import os, inspect
from src import *

csv_path=os.path.join(this_file_path, "..", "..", "nypd-arrest-data-2018-1.csv") 
csv_path=os.path.normpath(csv_path)
nypd_list=load_nypd_data(csv_path)
requirement_1(nypd_list)
requirement_2(nypd_list)
requirement_3(nypd_list)
requirement_4(nypd_list)
sql="select * from nypd_data limit 10"
print("\nHere are the first 10 rows from the database...\n")
results=test_database(sql)
print(results)