from load_data import csv_path, load_nypd_data

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