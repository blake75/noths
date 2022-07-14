import os, inspect
from csv import reader, writer
import sqlite3
from sqlite3 import Error

this_file_path=os.path.abspath(inspect.getsourcefile(lambda:0)) #location of this file
outputs_path=os.path.join(this_file_path, "..", "..", "outputs") 
outputs_path=os.path.normpath(outputs_path)

#Use a schema, it gives us more control in loading the data
nypd_schema={
    0: {"colname":'ARREST_KEY', "type": "int"},
    1: {"colname":'ARREST_DATE', "type": "str"},
    2: {"colname":'PD_CD', "type": "int"},
    3: {"colname":'PD_DESC', "type": "str"},
    4: {"colname":'KY_CD', "type": "int"},
    5: {"colname":'OFNS_DESC', "type": "str"},
    6: {"colname":'LAW_CODE', "type": "str"},
    7: {"colname":'LAW_CAT_CD', "type": "str"},
    8: {"colname":'ARREST_BORO', "type": "str"},
    9: {"colname":'ARREST_PRECINCT', "type": "int"},
    10: {"colname":'JURISDICTION_CODE', "type": "int"},
    11: {"colname":'AGE_GROUP', "type": "str"},
    12: {"colname":'PERP_SEX', "type": "str"},
    13: {"colname":'PERP_RACE', "type": "str"},
    14: {"colname":'X_COORD_CD', "type": "float"},
    15: {"colname":'Y_COORD_CD', "type": "float"},
    16: {"colname":'Latitude', "type": "float"},
    17: {"colname":'Longitude', "type": "float"}
}

def load_nypd_data(csv_path, schema=nypd_schema):
    '''Load the data using the nyp_schema. Ensure data_types are stored as required'''
    print("\nLoading the NYPD data. Please wait a few moments...")
    with open(csv_path, 'r') as f:
        csv_reader = reader(f)
        data=[]
        for i, row in enumerate(csv_reader):
            rec=[]
            if i==0: 
                num_cols=len(row)
                rec=row
            else:
                for col_no in range(0, num_cols):
                    data_type=schema[col_no]["type"]
                    if row[col_no]: 
                        val=eval(data_type)(row[col_no])
                        if col_no==1: val=val[0:10] #Trim the time off datetime, it's defaulted to 12am anyway
                    else: #Need to store an appropriate placeholder for blank values
                        if data_type=="str": val=""
                        elif data_type in ("int", "float"): val=float('NaN')
                    rec.append(val)
            data.append(rec)
    print("\nData Loaded")
    return data

def groupby_count(nypd_data:list, group_by_cols:list)->dict: 
    '''Function to count by any combination of group variables'''
    dict_data={}
    for arrest_key, arrest_date, pd_cd, pd_desc, ky_cd, ofns_desc, law_code, law_cat_cd, arrest_boro, arrest_precinct, \
        jurisdiction_code, age_group, perp_sex, perp_race, x_coord_cd, y_coord_cd, latitude, longitude \
        in nypd_data[1:]:
        compos_key=[]
        for col in group_by_cols:
            compos_key.append(eval(col.lower()))
        compos_key=tuple(compos_key)
        dict_data[compos_key]=dict_data.get(compos_key,0)+1
    dict_data={k: v for k, v in sorted(dict_data.items(), key=lambda item: item[1], reverse=True)}
    return dict_data

def requirement_1(nypd_list, top:int=10):
    print("\nFulfilling Requirement 1\n")
    title=f"Here are the top {top} offenses from the 2018 NYPD data:-\n"
    by_offense_dict=groupby_count(nypd_list, ["OFNS_DESC"])
    offenses=list(by_offense_dict.items())[:top]
    for i, top_offense in enumerate(offenses):
        if not i: print(title)
        group_by, arrests = top_offense
        offense_type=group_by[0]
        print(f"{offense_type.capitalize()}: {arrests:,} arrests")
    return offenses

def requirement_2(nypd_list, nth_rec:int=4):
    '''This has a list comprehension'''
    print("\nFulfilling Requirement 2\n")
    suf = lambda n: "%d%s"%(n,{1:"st",2:"nd",3:"rd"}.get(n if n<20 else n%10,"th")) #add st, rd, th to the number for readibility
    title=f"Here is the record for the {suf(nth_rec)} greatest number of arrests by PD_CD for each age group:-\n"
    by_age_pdcd_dict=groupby_count(nypd_list, ["AGE_GROUP", "PD_CD"])
    age_group_order={"18-24": 2, "<18": 1, "25-44": 3, "45-64": 4, "65+": 5}
    by_age_pdcd_list=[(age_group_order.get(age_group, -1) ,age_group, pd_cd, arrests) #Here is a list comprehension \
            for (age_group, pd_cd), arrests \
            in by_age_pdcd_dict.items()\
            if pd_cd]
    by_age_pdcd_list.sort(key=lambda tup: (tup[0], 0-tup[3]))
    prev_age_group_id=0
    print(title)
    results=""
    for age_group_id, age_group, pd_cd, arrests in by_age_pdcd_list:
        if age_group_id > prev_age_group_id: counter=0
        counter+=1
        if counter==nth_rec:
            this_result=f"In the age group '{age_group}' for PD_CD '{pd_cd}' there were {arrests:,} arrests" + "\n"
            results+=this_result
            print(this_result)
        prev_age_group_id=age_group_id
    return results

def requirement_3(nypd_list,
    fullfilepath=os.path.join(outputs_path, 'filtered_ofns_desc.csv')
    ):
    print("\nFulfilling Requirement 3\n")
    match=input("Enter the value of OFNS_DESC (offense description) you would like to filter on:\n >")
    headers=nypd_list[0]
    filt_nypd_list=[]
    for rec in nypd_list[1:]:
        if match.upper() in rec[5]:
            filt_nypd_list.append(rec)
    if len(filt_nypd_list)==0:
        print(f"No records have been found for OFNS_DESC '{match}''")
        return
    with open(fullfilepath, 'w', newline='') as f: 
        write = writer(f)
        write.writerow(headers) 
        write.writerows(filt_nypd_list) 
    print(f"The file has been written to '{fullfilepath}'")
    return fullfilepath    

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