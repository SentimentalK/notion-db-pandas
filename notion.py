import os
import re
import time
import json
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
            "formula": lambda x: self.find_formula(x),
            "email": lambda x: x if x else None,
            "phone_number": lambda x: x if x else None,

        }
        self.mutator = {
            "number": lambda x:x ,
            "date": lambda x: {"start": x, "end": None, "time_zone": None },
            # "Name": {"title": [{"text": {"content": "Updated Name"}}]},
            # "Tags": {"multi_select": [{"name": "Tag1"}, {"name": "Tag2"}]},
            # "Select": {"select": {"name": "Option1"}},
            # "Checkbox": {"checkbox": True},
            # "URL": {"url": "https://example.com"},
            # "Email": {"email": "example@example.com"},
            # "Phone": {"phone_number": "+1234567890"},
            # "Formula": {"formula": {"string": "Result"}},
            # "Relation": {"relation": [{"id": "page_id_here"}]},
            # "Rollup": {"rollup": {"number": 10, "type": "number"}},
            # "People": {"people": [{"id": "user_id_here"}]},
            # "Files": {"files": [{"name": "file.pdf", "type": "external", "external": {"url": "https://example.com/file.pdf"}}]},
            "rich_text": lambda x: [{"text": {"content": x}}],
            "status": lambda x:{"name": x}
        }
        self.constants = ["formula"]

    def find_formula(self, data):
        supported = ['number','string']
        if data['type'] not in supported:
            print(f'data formula - {data['type']} not supported. returning None')
            return None
        return data[data['type']]
    
    def find_rollup(self, column):
        if column not in self.relations:
            raise ValueError(f"rollup column \"{column}\" not defind in Table relations.")
        relations = [i for i,v in self.schemas.items() if v == 'relation']
        related = [i for i in relations if id(self.relations[i]["from_table"]) == id(self.relations[column]["from_table"])]
        if not related:
            raise ValueError(f"check refered table for column {column}. it is probably wrong.")
        return related[0]

    def reads(self):
        url = f"https://api.notion.com/v1/databases/{self.database_id}/query"
        self.origin = requests.post(url, headers=self.headers).json()
        while self.origin['has_more']:
            payload = {"page_size": 100}
            payload["start_cursor"] = self.origin['next_cursor']
            r = requests.post(url, headers=self.headers, json=payload).json()
            self.origin['results'] += r['results']
            self.origin['has_more'] = r['has_more']
            time.sleep(0.1)
        self._load_to_pandas()
    
    def write(self, where_notion_id, SET, TO):
        #ToDo: datachecker, when assign number it can't be string
        url = f"https://api.notion.com/v1/pages/{where_notion_id}"
        data_type = self.schemas[SET]
        if data_type in self.constants:
            print(f"You suppose not to modify the data_type {self.constants},\n  you are modifying {SET} TO {TO}. SKIPT.")
            return
        try:
            data = { "properties": { SET: {data_type: self.mutator[data_type](TO)} } }
        except KeyError:
            raise UserWarning(f"Framework doesn't support update data_type {data_type}.\n  you are modifying {SET} TO {TO}. SKIPT.")
        time.sleep(0.1)
        return requests.patch(url, headers=self.headers, data=json.dumps(data))

    def writes(self, with_reference_table=True):
        columns = [i for i in self._df.columns if "|" not in i]
        diff =  self._df[columns].compare(self.merged_df[columns])
        changes = []
        if not diff.empty:
            for col in diff.columns.levels[0]:
                for idx in diff.index:
                    if pd.notna(diff.at[idx, (col, 'self')]) or pd.notna(diff.at[idx, (col, 'other')]):
                        changes.append({
                            "notion_id": self._df.at[idx, "notion_id"],
                            "column": col,
                            "new_value": diff.at[idx, (col, 'self')],
                            "old_value": diff.at[idx, (col, 'other')]
                        })
            for change in changes:
                if pd.isna(change['new_value']):
                    change['new_value'] = None 
                r = self.write(where_notion_id = change['notion_id'], 
                           SET = change['column'],
                           TO = change['new_value'])
                if hasattr(r, 'status_code'):
                    print(f"<{r.status_code}>: notion_id {change['notion_id']}, SET {change['column']} FROM {change['old_value']} TO {change['new_value']}")
                    if r.status_code != 200:
                        print(r.json()['message'])
        else:
            print(f"No update for table:{self.table_name}")
        self.merged_df.update(self.df)
        if with_reference_table:
            self.write_reference_tables()

    def write_reference_tables(self):
        if self.relations:
            for relation in self.relations:
                if relation in list(chain(*self.mapping_relations().values())):
                    continue
                table = self.relations[relation]["from_table"]
                columns = [i for i in self.merged_df.columns if re.match(rf'^{relation}\|',i)]
                temp = self.merged_df[columns].copy()
                temp.set_index(f'{relation}|notion_id', drop=False, inplace=True)
                temp.columns = temp.columns.str.removeprefix(f"{relation}|")
                table.df.update(temp)
                table.writes()

    def update(self, WHERE, IS, SET, TO):
        self.df.loc[self._df[WHERE] == IS, SET] = TO

    def update_where_index(self, IS, SET, TO):
        self.df.loc[IS, SET] = TO


class Table(Notion):

    def __init__(self, database_id, relations=None):
        super().__init__()
        self.table_name = database_id[-8:]
        self.columns_with_default_value = ["notion_id", "unique_id", "status"]
        self.database_id = database_id
        self.schemas = {}
        self.relations = relations
        self._df = None
        self._merged_df = None
    
    @property
    def df(self):
        if self._df is None:
            self.reads()
        return self._df
    
    @property
    def merged_df(self):
        if self._merged_df is None:
            self.df
        return self._merged_df

    def mapping_relations(self):
        relations = {column:[] for column,type in self.schemas.items() if type == "relation"}
        for column, type in self.schemas.items(): 
            if type == "rollup":
                relations[self.accessor[type](column)] += [column]
        return relations
    
    def _load_to_pandas(self):
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
        self._df = pd.DataFrame(df)
        
        # clean empty lines
        defaults = [i for i in self.schemas.values() if i in self.columns_with_default_value]+["notion_id"]
        self._df = self._df[~(self._df.isna().sum(axis=1)==len(self._df.columns)-len(defaults))]

        if self.relations:
            relations = self.mapping_relations()
            for relation,rollups in relations.items():
                ref_df = self.relations[relation]["from_table"]
                relation_column = self.relations[relation]["lookup_column"]
                self._df = self._df.merge(ref_df.merged_df.add_prefix(f'{relation}|'), 
                    left_on = relation,
                    right_on= f'{relation}|notion_id',
                    how='left')
        self._df = self._df.set_index('notion_id', drop=False)
        self._merged_df = self._df.copy()
        #ToDo: alert user when their relation not same with the data on notion.
        if self.relations:
            columns = [f"{relation}|{self.relations[i]["lookup_column"]}" for relation,rollups in self.mapping_relations().items() for i in rollups] \
                 +[f"{relation}|{self.relations[relation]["lookup_column"]}" for relation in self.mapping_relations()]\
                 +[i for i in self._merged_df.columns if "|" not in i and i not in [item for key, values in relations.items() for item in [key] + values]]
            self._df = self._merged_df[columns]

