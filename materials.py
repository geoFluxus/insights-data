import utils
import pandas as pd
import variables as var
import json
import os


# VARIABLES
VARS = {
    'INPUT_DIR': var.INPUT_DIR,
    'AREA_DIR': var.AREA_DIR,
    'AREA': var.AREA,
    'LEVEL': var.LEVEL,
    'YEAR': var.YEAR,
    'COROP_FILE': var.COROP_FILE,
    'COROPS': var.COROPS,
    'OUTPUT_DIR': var.OUTPUT_DIR,
    'TRANSITION_AGENDAS_UNIT': var.UNITS['MATERIALS']['TRANSITION_AGENDAS'],
    'MATERIAL_TREE_UNIT': var.UNITS['MATERIALS']['MATERIAL_TREE']
}

DATA = {}

MATERIAL_TREE = {}


def process_lma():
    # process LMA ontvangst
    for typ in ['Ontvangst']:
        # data prefix
        prefix = f"{PREFIXES[VARS['LEVEL']]}\t{PREFIXES[typ]} afval"

        # import file
        print()
        print(f'Import {typ}...')
        path = f"{VARS['INPUT_DIR']}/{VARS['AREA_DIR']}/LMA/processed"
        filename = f"{path}/{typ.lower()}_{VARS['AREA'].lower()}_{VARS['YEAR']}_full.csv"
        df = pd.read_csv(filename, low_memory=False)

        # add areas to roles
        print('Add areas to roles...')
        source = ROLES[typ]['source']  # source role
        df = utils.add_areas(df,
                             areas=polygon,
                             role=source,
                             admin_level=VARS['LEVEL'])

        # add classifications
        for name, classif in ewc_classifs.items():
            df = utils.add_classification(df, classif, name=name,
                                          left_on='EuralCode',
                                          right_on='ewc')

        # TRANSITION AGENDAS
        DATA[f"{prefix}\ttransition_agendas\t{VARS['YEAR']}"] = \
            utils.get_classification_graphs(df,
                                            source=source,
                                            level=VARS['LEVEL'],
                                            area=VARS['AREA'],
                                            klass='agendas',
                                            unit=VARS['TRANSITION_AGENDAS_UNIT'])

        # MATERIAL SANKEY
        # also retrieve data for material tree
        DATA[f"{prefix}\tmaterial_sankey\t{VARS['YEAR']}"], hierarchy, sums = \
            utils.get_material_sankey(df,
                                      source=source,
                                      level=VARS['LEVEL'],
                                      area=VARS['AREA'],
                                      unit=VARS['MATERIAL_TREE_UNIT'])

        # store material tree data
        MATERIAL_TREE['afval'] = {
            'hierarchy': hierarchy,
            'sums': sums
        }


def process_cbs():
    print()
    print('Import CBS data...')

    # stromen -> million kg
    prefix = f"COROP\tgoederen"
    path = f"{VARS['INPUT_DIR']}/{VARS['AREA_DIR']}/CBS"
    filename = f"{path}/{VARS['COROP_FILE']}.csv"
    df = pd.read_csv(filename, low_memory=False)
    df['Gewicht_KG'] = df['Brutogew'] * 10 ** 6
    df['Gewicht_KG'] = df['Gewicht_KG'].astype('int64')

    # filter by year & COROPS
    # exclude chapter 24 (Afval)
    df = df[
        (df['Jaar'] == VARS['YEAR']) &
        (df['Regionaam'].isin(VARS['COROPS'])) &
        (df['Goederengroep_nr'] != 24)
    ]

    # import cbs classifications
    cbs_classifs = {}
    for classif in ['agendas', 'materials']:
        file_path = f"{VARS['INPUT_DIR']}/{VARS['AREA_DIR']}/extra/cbs_{classif}.csv"
        if not os.path.isfile(file_path):
            file_path = f"{VARS['INPUT_DIR']}/DATA/ontology/cbs_{classif}.csv"
        cbs_classifs[classif] = pd.read_csv(file_path, low_memory=False, sep=';')

    # add classifications
    for name, classif in cbs_classifs.items():
        df = utils.add_classification(df, classif, name=name,
                                      left_on='Goederengroep_nr',
                                      right_on='cbs')

    # TRANSITION AGENDAS
    # filter CBS input
    input_df = df[df['Stroom'].isin([
        'Aanbod_eigen_regio',
        'Invoer_internationaal',
        'Invoer_regionaal'
    ])]
    DATA[f"{prefix}\ttransition_agendas\t{VARS['YEAR']}"] = \
        utils.get_classification_graphs(input_df,
                                        area=VARS['COROPS'][0],
                                        klass='agendas',
                                        unit=VARS['TRANSITION_AGENDAS_UNIT'])

    # MATERIAL SANKEY
    # also retrieve data for material tree
    DATA[f"{prefix}\tmaterial_sankey\t{VARS['YEAR']}"], hierarchy, sums = \
        utils.get_material_sankey(input_df,
                                  level=VARS['LEVEL'],
                                  area=VARS['COROPS'][0],
                                  unit=VARS['MATERIAL_TREE_UNIT'])

    # store material tree data
    MATERIAL_TREE['goederen'] = {
        'hierarchy': hierarchy,
        'sums': sums
    }


def merge_material_trees(unit='kg'):
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
                "value": utils.kg_to_unit(
                    item['sums'].get(k, 0),
                    unit=unit
                )
                # "value": round(item['sums'].get(k, 0) / total[k] * 100)
            })

    # convert to tree visualization
    def update_tree(tree, dic):
        for key in dic.keys():
            item = {
                "name": key,
                'values': sums[key],
                "unit": unit
            }
            if isinstance(dic[key], dict):
                item["children"] = []
                item = update_tree(item, dic[key])
            tree.setdefault("children", []).append(item)
        return tree
    tree = update_tree({}, hierarchy)["children"][0]
    DATA[f"province\tall\tmaterial_tree\t{VARS['YEAR']}"] = [{
        "data": tree
    }]

    # convert tree to table
    terms = {
        'afval': 'amount_waste',
        'goederen': 'amount_goods'
    }
    def tree_to_table(dic, parent=None, table=[], id=1):
        for key in dic.keys():
            item = {
                'key': id,
                'material': key,
                'parent': parent,
                "unit": unit
            }
            for sum in sums[key]:
                item[terms[sum['type']]] = sum['value']
            table.append(item)
            id += 1
            if isinstance(dic[key], dict):
                table, id = tree_to_table(dic[key], parent=key, table=table, id=id)
        return table, id
    table, id = tree_to_table(hierarchy)
    DATA[f"province\tall\tmaterial_table\t{VARS['YEAR']}"] = [{
        "data": table,
    }]


if __name__ == "__main__":
    ROLES = var.ROLES
    PREFIXES = var.PREFIXES

    # start analysis
    print('MATERIALS ANALYSIS')
    print('VARIABLES:')
    for name, value in VARS.items():
        print(f'{name}={value}')

    # import areas
    # import province polygon
    polygon = utils.import_areas(level=VARS['LEVEL'])
    polygon = polygon[polygon['name'] == VARS['AREA']]
    assert len(polygon) == 1

    # import ewc classifications
    ewc_classifs = {}
    for classif in ['agendas', 'materials']:
        file_path = f"{VARS['INPUT_DIR']}/{VARS['AREA_DIR']}/extra/ewc_{classif}.csv"
        if not os.path.isfile(file_path):
            file_path = f"{VARS['INPUT_DIR']}/DATA/ontology/ewc_{classif}.csv"
        ewc_classifs[classif] = pd.read_csv(file_path, low_memory=False, sep=';')

    # process LMA data
    process_lma()

    # process CBS data
    if len(VARS['COROPS']):
        process_cbs()

    # merge material trees
    merge_material_trees(unit=VARS['MATERIAL_TREE_UNIT'])

    # GRAPHS
    utils.export_graphs(f"{VARS['OUTPUT_DIR']}/materials.json", data=DATA)
