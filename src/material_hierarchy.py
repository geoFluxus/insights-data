import pandas as pd
from src.analysis import utils
import json
import csv


path = r"W:\Shared With Me\MASTER\DATA\ontology\cbs_material_ontology.xlsx"
df = pd.read_excel(path)
hierarchy = {}
for idx, row in df.iterrows():
    material = row['Materiaal']
    parent = row['Hoofdgroep']

    item = utils.search_nested(parent, hierarchy)
    if item is None:
        hierarchy[material] = {}
    else:
        utils.update_nested(hierarchy, parent, {
            **item,
            material: {}
        })

path = fr"W:\Shared With Me\MASTER\DATA\ontology\cbs_materials.csv"
ontology = pd.read_csv(path, sep=';')

with open('cbs_materials.csv', mode='w', encoding='utf-8', newline="") as outfile:
    writer = csv.writer(outfile, delimiter=';')
    writer.writerow(['cbs', 'name', 'materials'])
    for cbs, name in zip(
        ontology['cbs'].to_list(),
        ontology['name'].to_list()
    ):
        materials = utils.path_nested(name, hierarchy)
        if materials is None: materials = []
        writer.writerow([cbs, name, '+'.join(materials)])

print(json.dumps(hierarchy, indent=2))
print(utils.path_nested("Biotisch Polymeer", hierarchy))