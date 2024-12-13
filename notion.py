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
            "formula": lambda x: self.find_formula(x)
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
            # "Rich_text": {"rich_text": [{"text": {"content": "Rich text content"}}]},
            # "Status": {"status": {"name": "In Progress"}}
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
        related = [column for column in self.relations if id(self.relations[column]["from_table"]) == id(self.relations[column]["from_table"])]
        return related[0]

    def reads(self):
        url = f"https://api.notion.com/v1/databases/{self.database_id}/query"
        self.origin = requests.post(url, headers=self.headers).json()
    
    def write(self, where_notion_id, set, to):
        url = f"https://api.notion.com/v1/pages/{where_notion_id}"
        data_type = self.schemas[set]
        if data_type in self.constants:
            print(f"You suppose not to modify the data_type {self.constants},\n  you are modifying {set} TO {to}. SKIPT.")
            return
        try:
            data = { "properties": { set: {data_type: self.mutator[data_type](to)} } }
        except KeyError:
            print(f"Framework doesn't support update data_type {data_type}.\n  you are modifying {set} TO {to}. SKIPT.")
            return
        time.sleep(0.1)
        return requests.patch(url, headers=self.headers, data=json.dumps(data))

    def writes(self, with_reference_table=True):
        diff =  self.df.compare(self.merged_df[self.df.columns])
        changes = []
        if not diff.empty:
            for col in diff.columns.levels[0]:
                for idx in diff.index:
                    if pd.notna(diff.at[idx, (col, 'self')]) or pd.notna(diff.at[idx, (col, 'other')]):
                        changes.append({
                            "notion_id": self.df.at[idx, "notion_id"],
                            "column": col,
                            "new_value": diff.at[idx, (col, 'self')],
                            "old_value": diff.at[idx, (col, 'other')]
                        })
            for change in changes:
                r = self.write(where_notion_id = change['notion_id'], 
                           set = change['column'],
                           to = change['new_value'])
                if r:
                    print(f"<{r.status_code}>: notion_id {change['notion_id']}, SET {change['column']} FROM {change['old_value']} TO {change['new_value']}")
                    if r.status_code != 200:
                        print(r.content)
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
                temp.columns = temp.columns.str.removeprefix(f"{relation}|")
                table.df.update(temp)
                table.writes()

    def update(self, WHERE, IS, SET, TO):
        self.df.loc[self.df[WHERE] == IS, SET] = TO


class Table(Notion):

    def __init__(self, database_id, relations=None):
        super().__init__()
        self.table_name = database_id[-8:]
        self.columns_with_default_value = ["notion_id", "unique_id", "status"]
        self.database_id = database_id
        self.schemas = {}
        self.relations = relations
        self.reads()
        self.load_to_pandas()

    def mapping_relations(self):
        relations = {column:[] for column,type in self.schemas.items() if type == "relation"}
        for column, type in self.schemas.items(): 
            if type == "rollup":
                relations[self.accessor[type](column)] += [column]
        return relations
    
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
        columns = []
        if self.relations:
            relations = self.mapping_relations()
            for relation,rollups in relations.items():
                ref_df = self.relations[relation]["from_table"]
                relation_column = self.relations[relation]["lookup_column"]
                columns += [f'{relation}|{i}' for i in ["notion_id",relation_column]+rollups]
                self.df = self.df.merge(ref_df.merged_df.add_prefix(f'{relation}|'), 
                    left_on = relation,
                    right_on= f'{relation}|notion_id',
                    how='left')
        self.df = self.df.set_index('notion_id', drop=False)
        self.merged_df = self.df.copy()
        if columns:
            columns = [i for i in self.merged_df.columns 
                       if "|" not in i 
                       or (i in columns and "|notion_id" not in i)]
            self.df = self.merged_df[columns]

