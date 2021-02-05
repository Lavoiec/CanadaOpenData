
import json
import pandas as pd
import numpy as np


class OpenDataResults:

    def __init__(self, path):
        self.raw_json = self.read_json(path)
        self.search_results = self.get_search_results()

    def read_json(self, path):
        with open(path, 'r') as j_file:
            return json.load(j_file)


    def get_search_results(self):
        """
        Note that depending on the query itself, it may have been split into an array of 
        json objects. This is because of the request limit on the API (1000 results per request).
        We can flatten this pretty easily.
        """
        s_results = []
        for i in range(len(self.raw_json)):
            s_results += self.raw_json[i]["result"]["results"]
        return s_results

    def create_dataframe_from_attr(self, key): 
        attr_list = []

        for o in self.search_results:
            try:
                o_key = o[key]
                
                if type(o_key) == dict:
                    obj_key = o_key
                else:
                    obj_key = {key: o_key}

                obj_key["parent_id"] = o["id"]
            except KeyError:
                obj_key = np.nan

            attr_list.append(obj_key)
        try:
            df = pd.DataFrame(attr_list)
            return df
        except:
            return "Try harder than that"

    def get_metadata(self, df=True):
        metadata_files = []
        for result in self.search_results:
            for resource in result["resources"]:
                # add an id from the original source so we don't forget what goes where
                resource["parent_id"] = result["id"]
                metadata_files.append(resource)
        if df:
            return pd.DataFrame(metadata_files)
        
        return metadata_files

    def get_attrs(self):
        list_of_keys = []
        for obj in self.search_results:
            list_of_keys += obj.keys()
        return list(set(list_of_keys))


def analysis_pipeline(path):
    results = OpenDataResults(path)
    data_name = results.create_dataframe_from_attr("title")
    data_collection = results.create_dataframe_from_attr("collection")

    data = pd.merge(data_name, data_collection, on="parent_id")

    organization = results.create_dataframe_from_attr("organization")
    data = pd.merge(data, organization,on="parent_id", suffixes=("", "_org"))
    metadata = results.get_metadata()
    keywords = results.create_dataframe_from_attr("keywords").explode("en")


    return [results, data, metadata, keywords]

