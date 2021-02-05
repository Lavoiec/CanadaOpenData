import requests
import json
import pandas
import math

def get_all_results(q):
    # example
    # https://open.canada.ca/data/en/api/3/action/package_search?q=water&rows=1893
    # Step 1. Determine the number of rows
    query = requests.get("https://open.canada.ca/data/en/api/3/action/package_search?q="+q+"&rows=1").json()
    number_of_results = query["result"]["count"]
    # number of offsets we will need to be used to get all results...
    paginations_required = math.floor(number_of_results / 1000) + 1
    offset_indicator = [1000*i for i in range(paginations_required)]
    query_results = []
    for offset in offset_indicator:
        query = "https://open.canada.ca/data/en/api/3/action/package_search?q="+q+"&rows=1000&start="+str(offset)
        result = requests.get(query).json()
        query_results.append(result)
    return query_results

def write_json(json_object, name):
    out_file = open("E:/Documents/Python_Scripts/dataportalstuff/data/"+name, "w") 
    json.dump(json_object, out_file, indent = 6) 
    out_file.close()




#water_json = get_all_results("water")
#write_json(water_json, "water_query.json")

#water_quality_json = get_all_results("water quality")
#write_json(water_quality_json, "water_quality_query.json")

#water_quantity_json = get_all_results("water quantity")
#write_json(water_quantity_json,"water_quantity_query.json")

#water_use_json = get_all_results("water use")
#write_json(water_use_json, "water_use_query.json")

#ecosystem_json = get_all_results("ecosystem")
#write_json(ecosystem_json, "ecosystem_query.json")
