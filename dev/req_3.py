import os, inspect
from csv import writer
from load_data import csv_path, load_nypd_data

this_file_path=os.path.abspath(inspect.getsourcefile(lambda:0)) #location of this file
outputs_path=os.path.join(this_file_path, "..", "..", "outputs") 
outputs_path=os.path.normpath(outputs_path)

nypd_list=load_nypd_data(csv_path)

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

requirement_3(nypd_list)