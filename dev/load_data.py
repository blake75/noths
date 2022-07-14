import os, inspect
from csv import reader

this_file_path=os.path.abspath(inspect.getsourcefile(lambda:0)) #location of this file
csv_path=os.path.join(this_file_path, "..", "..", "nypd-arrest-data-2018-1.csv") 
csv_path=os.path.normpath(csv_path)

print(csv_path)

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

data=load_nypd_data(csv_path)
print(data[0:10])