import utils
import pandas as pd
import variables as var
import json


# INPUTS
PROVINCE = "Utrecht"
YEAR = 2019
COROPS = [
    'Utrecht'
]

DATA = {}

MATERIAL_TREE = {}


def process_lma():
    # process LMA ontvangst
    for typ in ['Ontvangst']:
        # data prefix
        prefix = f"{PREFIXES['Provincie']}\t{PREFIXES[typ]} waste"

        # import file
        print()
        print(f'Import {typ}...')
        path = f'../../../../../media/geofluxus/DATA/national/{PROVINCE.lower()}/processed'
        filename = f'{path}/{typ.lower()}_{PROVINCE.lower()}_{YEAR}.csv'
        df = pd.read_csv(filename, low_memory=False)

        # add areas to roles
        print('Add areas to roles...')
        source = ROLES[typ]['source']  # source role
        df = utils.add_areas(df,
                             areas=polygon,
                             role=source,
                             admin_level='Provincie')

        # add classifications
        for name, classif in ewc_classifs.items():
            df = utils.add_classification(df, classif, name=name,
                                          left_on='EuralCode',
                                          right_on='ewc')

        # TRANSITION AGENDAS
        DATA[f'{prefix}\ttransition_agendas\t{YEAR}'] = \
            utils.get_classification_graphs(df,
                                            source=source,
                                            level='Provincie',
                                            area=PROVINCE,
                                            klass='agendas')

        # MATERIAL SANKEY
        # also retrieve data for material tree
        DATA[f'{prefix}\tmaterial_sankey\t{YEAR}'], hierarchy, sums = \
            utils.get_material_sankey(df,
                                      source=source,
                                      level='Provincie',
                                      area=PROVINCE)

        # store material tree data
        MATERIAL_TREE['EWC'] = {
            'hierarchy': hierarchy,
            'sums': sums
        }


def process_cbs():
    print()
    print('Import CBS data...')

    # stromen -> million kg
    prefix = f"{PREFIXES['Provincie']}\tmaterial"
    path = './data/cbs/Tabel Regionale stromen 2015-2019.csv'
    df = pd.read_csv(path, low_memory=False, sep=';')

    # filter by year & COROPS
    df = df[(df['Jaar'] == YEAR) & (df['COROP_naam'].isin(COROPS))]

    # import cbs classifications
    cbs_classifs = {}
    for classif in ['agendas', 'materials']:
        cbs_classifs[classif] = pd.read_csv(f'./data/materials/cbs_{classif}.csv', low_memory=False, sep=';')

    # add classifications
    for name, classif in cbs_classifs.items():
        df = utils.add_classification(df, classif, name=name,
                                      left_on='Goederengroep_nr',
                                      right_on='cbs')

    # TRANSITION AGENDAS
    # filter CBS input
    df['Gewicht_KG'] = df['Brutogew'] * 10**6
    df['Gewicht_KG'] = df['Gewicht_KG'].astype('int64')
    input_df = df[df['Stroom'].isin([
        'Aanbod_eigen_regio',
        'Invoer_internationaal',
        'Invoer_regionaal'
    ])]
    DATA[f'{prefix}\ttransition_agendas\t{YEAR}'] = \
        utils.get_classification_graphs(input_df,
                                        area=PROVINCE,
                                        klass='agendas')

    # MATERIAL SANKEY
    # also retrieve data for material tree
    DATA[f'{prefix}\tmaterial_sankey\t{YEAR}'], hierarchy, sums = \
        utils.get_material_sankey(input_df,
                                  level='Provincie',
                                  area=PROVINCE)

    # store material tree data
    MATERIAL_TREE['CBS'] = {
        'hierarchy': hierarchy,
        'sums': sums
    }


def merge_material_trees():
    # reset dic to only nested dics
    # to allow merge
    def reset_nested(dic):
        for key in dic.keys():
            if isinstance(dic[key], dict):
                reset_nested(dic[key])
            else:
                dic[key] = {}
        return dic

    # merge material tree hierarchies
    hierarchy = {}
    for typ, item in MATERIAL_TREE.items():
        new_hierarchy = reset_nested(item['hierarchy'])
        hierarchy = utils.merge_nested(new_hierarchy, hierarchy)

    # add up amounts for each hierarchy level
    sums = {}
    for typ, item in MATERIAL_TREE.items():
        for k in utils.flatten_nested(hierarchy):
            sums.setdefault(k, []).append({
                "type": typ,
                "value": item['sums'].get(k, 0)
                # "value": round(item['sums'].get(k, 0) / total[k] * 100)
            })

    # convert to tree visualization
    def update_tree(tree, dic):
        for key in dic.keys():
            item = {
                "name": key,
                'values': sums[key]
            }
            if isinstance(dic[key], dict):
                item["children"] = []
                item = update_tree(item, dic[key])
            tree.setdefault("children", []).append(item)
        return tree
    tree = update_tree({}, hierarchy)["children"][0]
    DATA[f'province\tall\tmaterial_tree\t{YEAR}'] = [{"data": tree}]

    # convert tree to table
    terms = {
        'EWC': 'amount_waste',
        'CBS': 'amount_goods'
    }
    def tree_to_table(dic, parent=None, table=[], id=1):
        for key in dic.keys():
            item = {
                'key': id,
                'material': key,
                'parent': parent
            }
            for sum in sums[key]:
                item[terms[sum['type']]] = sum['value']
            table.append(item)
            id += 1
            if isinstance(dic[key], dict):
                table, id = tree_to_table(dic[key], parent=key, table=table, id=id)
        return table, id
    table, id = tree_to_table(hierarchy)
    DATA[f'province\tall\tmaterial_table\t{YEAR}'] = [{"data": table}]


if __name__ == "__main__":
    ROLES = var.ROLES
    PREFIXES = var.PREFIXES

    # import areas
    # import province polygon
    polygon = utils.import_areas(level='provincies')
    polygon = polygon[polygon['name'] == PROVINCE]
    assert len(polygon) == 1

    # import ewc classifications
    ewc_classifs = {}
    for classif in ['agendas', 'materials']:
        ewc_classifs[classif] = pd.read_csv(f'./data/materials/ewc_{classif}.csv',
                                            low_memory=False,
                                            sep=';')

    # start analysis
    print(f'YEAR: {YEAR}')

    # process LMA data
    process_lma()

    # process CBS data
    process_cbs()

    # merge material trees
    merge_material_trees()

    # GRAPHS
    utils.export_graphs('./test/materials.json', data=DATA)

