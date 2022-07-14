from load_data import load_nypd_data, csv_path
from groupby_count import groupby_count

nypd_list=load_nypd_data(csv_path)

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

requirement_1(nypd_list)
requirement_2(nypd_list)
