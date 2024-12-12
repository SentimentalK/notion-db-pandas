import os
import requests
import pandas as pd
from itertools import chain

if os.path.exists("token"):
    with open("token") as f:
        os.environ["NOTION_TOKEN"] = f.read().strip()


class Notion(object):

    def __init__(self):
        self.NOTION_TOKEN = os.environ["NOTION_TOKEN"]
        self.headers = {
            "Authorization": f"Bearer {self.NOTION_TOKEN}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28",
        }
        self.accessor = {
            "date": lambda x: x["start"] if x else None,
            "rich_text": lambda x: x[0]["text"]["content"] if x else None,
            "number": lambda x: x if x else None,
            "relation": lambda x: x[0]["id"] if x else None,
            "unique_id": lambda x: x["prefix"] + str(x["number"]) if x["prefix"] else str(x["number"]),
            "title": lambda x: x[0]["text"]["content"] if x else None,
            "select": lambda x: x["name"] if x else None,
            "status": lambda x: x["name"] if x else None,
            "rollup": lambda x: self.find_rollup(x),
            "formula": lambda x: self.find_formula(x)
        }

    def find_formula(self, data):
        supported = ['number','string']
        if data['type'] not in supported:
            print(f'data formula - {data['type']} not supported. returning None')
            return None
        return data[data['type']]
    
    def find_rollup(self, column):
        if column not in self.relations:
            raise ValueError(f"rollup column \"{column}\" not defind in Table relations.")
        related = [column for column in self.relations if id(self.relations[column]["from_table"]) == id(self.relations[column]["from_table"])]
        return related[0]


    def reads(self):
        url = f"https://api.notion.com/v1/databases/{self.database_id}/query"
        self.origin = requests.post(url, headers=self.headers).json()
    
    def writes(self):
        pass


class Table(Notion):

    def __init__(self, database_id, relations=None):
        super().__init__()
        self.table_name = database_id[-8:]
        self.columns_with_default_value = ["notion_id", "unique_id", "status"]
        self.database_id = database_id
        self.schemas = {}
        self.relations = relations
        if relations:
            self.column_mapping = self.mapping_columns()
        self.reads()
        self.load_to_pandas()
    
    def new(self, column):
        return f"{self.table_name}.{column}"
    
    def original(self, column):
        return column.split(".")[-1]

    def mapping_relations(self):
        relations = {column:[] for column,type in self.schemas.items() if type == "relation"}
        for column, type in self.schemas.items(): 
            if type == "rollup":
                relations[self.accessor[type](column)] += [column]
        return relations

    def mapping_columns(self):
        d = {}
        for k,v in self.mapping_relations():
            ref_df = v["from_table"]
            d[k] = v["lookup_column"]
        return d
    
    def load_to_pandas(self):
        df = []
        for d in self.origin["results"]:
            tmp = {}
            tmp["notion_id"] = d["id"]
            sorted_keys = sorted(d["properties"].keys(), key=lambda x:d["properties"][x]['type']=='rollup')
            for k in sorted_keys:
                t = d["properties"][k]["type"]
                v = d["properties"][k]
                if k not in self.schemas:
                    self.schemas[k] = t
                try:
                    if t != 'rollup':
                        tmp[k] = self.accessor[t](v[t])
                    else:
                        tmp[k] = tmp[self.accessor[t](k)]
                except KeyError as e:
                    print(f"Framework doesn't support data type {t} yet. Skip loading it.")
            df.append(tmp)
        self.df = pd.DataFrame(df)
        
        # clean empty lines
        defaults = [i for i in self.schemas.values() if i in self.columns_with_default_value]+["notion_id"]
        self.df = self.df[~(self.df.isna().sum(axis=1)==len(self.df.columns)-len(defaults))]
        
        if self.relations:
            relations = self.mapping_relations()
            for relation,rollups in relations.items():
                ref_df = self.relations[relation]["from_table"]
                relation_column = self.relations[relation]["lookup_column"]
                columns = ["notion_id",relation_column]+rollups
                self.df = self.df.merge(ref_df.df[columns].add_prefix(f'{relation}|'), 
                    left_on = relation,
                    right_on= f'{relation}|notion_id',
                    how='left')
            self.merged_df = self.df
            columns = [i for i in self.df.columns 
                       if i not in list(chain(*relations.items())) 
                       and "|notion_id" not in i
                    ]
            self.df = self.merged_df[columns]

