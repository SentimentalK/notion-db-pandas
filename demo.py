from notion import Table

PEOPLE = '15f5ba9898b680879f9bdcd3829cb499'
ODERS = '15f5ba9898b6801d89d1d5c82e08680b'
PRODUCTS = '15f5ba9898b6808bb91dc1432f1dd275'
COMPANIES = '15f5ba9898b68084aeccf2183bc2cb4f'

ppl = Table(PEOPLE)
comp = Table(COMPANIES)
product_company_relation = {
    # column name of the relation
    'company':{"from_table": comp, "lookup_column": "name"},
    'company_size' : {"from_table": comp, "lookup_column": "size"}
}
prod = Table(PRODUCTS, relations=product_company_relation)
orders_relation = {
    'seller':{"from_table": ppl, "lookup_column": "name"},
    'buyer':{"from_table": ppl, "lookup_column": "name"},
    'company':{"from_table": prod, "lookup_column": "company"},
    'product':{"from_table": prod, "lookup_column": "name"},
    'product_description' :{"from_table": prod, "lookup_column": "description"}
}
ords = Table(ODERS, relations=orders_relation)

# index is notion_id
# Console output:
# >>pandas_df.columns
# >>Index(['notion_id', 'order_date', 'order_status', 'price', 'order_id',
#          'product_description', 'company', 'product|name', 'buyer|name',
#          'seller|name', 'product_description|description'],
#         dtype='object')
pandas_df = ords.df

# react data with pandas
data_set = pandas_df[pandas_df['order_status']=='unpaid']
for notion_id, row in data_set.iterrows():
    pandas_df.loc[notion_id, 'price'] += 10
    # relation key in your relation object (not rollup column) and lookup_column will be combined for merged table
    pandas_df.loc[notion_id, 'product|description'] = f'change_description_{pandas_df.loc[notion_id, 'price']}'

# write back your changes to notion, all the related table will be automatically update.
# Console output:
#>> <200>: notion_id xxx, SET price FROM 122.49 TO 132.49
#   No update for table: ppl
#   No update for table: ppl
#   <200>: notion_id xxx, SET description FROM a TO change_description_132.49
#   No update for table: comp
ords.writes()