import pandas as pd
import geopandas as gpd
import numpy as np
import json
from src import _make_iterencode
import re
import variables as var


# parameters
INPUT_DIR = var.INPUT_DIR


def kg_to_unit(value, unit='kg', decimals=2):
    """
    converts kg to the desired unit
    """
    converters = {
        'kg': 1,
        't': 10**3,
        'kt': 10**6,
        'Mt': 10**9
    }
    return value / converters[unit]


def format_name(name):
    exclude = [
        'TransitieAgenda',
        'Materiaal'
    ]
    def replace(name):
        for e in exclude:
            name = name.replace(e, '')
        return name
    name = " ".join(
        re.findall('[A-Z&][^A-Z&]*', replace(name))
    ) \
        if any(char.isupper() for char in name) else name.capitalize().replace('_', ' ')
    return name


def export_graphs(fil, data=None):
    with open(fil, 'w') as outfile:
        # preprocess
        results = {}
        for key, items in data.items():
            # level, type, field, period = key.split('\t')
            for item in items:
                # item['level'] = level
                # item['period'] = period
                # item['type'] = type
                results.setdefault(key, []).append(item)

        json.encoder._make_iterencode = _make_iterencode._make_iterencode
        indent = (2, None)
        json.dump(results, outfile, indent=indent)


def import_areas(level=None):
    """
    import area polygons
    for different administration level
    (municipalities, provinces etc.)
    """
    LEVELS = {
        'Provincie': 'provincies',
        'Gemeente': 'gemeenten'
    }
    level = LEVELS[level]

    # load geometries
    areas = gpd.read_file(f'{INPUT_DIR}/GEODATA/areas/{level}/{level}_{var.YEAR}.shp')
    areas['centroid'] = areas['geometry'].centroid

    return areas


def add_areas(flows, areas=None, role=None, admin_level=None):
    """
    Add administrative areas to roles
    (point to polygon)
    """
    columns = list(flows)

    # join geolocation with area polygons
    flows['wkt'] = gpd.GeoSeries.from_wkt(flows[f'{role}_Location'])
    flows = gpd.GeoDataFrame(flows, geometry='wkt', crs='EPSG:4326')
    flows = gpd.sjoin(flows, areas, how='left', predicate='within')

    # keep only original dataframe columns & role area
    flows[f'{role}_{admin_level}'] = flows['name']
    columns.append(f'{role}_{admin_level}')
    flows = flows[columns]

    return flows


def add_classification(df, classif, name=None,
                       left_on=None, right_on=None):
    """
    Add ontology classifications
    to LMA & CBS data
    """
    columns = list(df.columns)
    classif[right_on] = classif[right_on].astype(str).str.zfill(6)
    df[left_on] = df[left_on].astype(str).str.zfill(6)
    df = pd.merge(df, classif, how='left', left_on=left_on, right_on=right_on)
    df.loc[df[name].isnull(), name] = 'Onbekend'
    columns.append(name)
    df = df[columns]
    return df


def compute_sankey_branch(flows,
                          source=None, source_in=True,
                          target=None, target_in=True,
                          level=None, areas=[],
                          unit='kg'):
    """
    Compute sankey brances
    for LMA & CBS data
    """
    binnen = {
        True: 'binnen',
        False: 'buiten'
    }

    conditions = []
    for area in areas:
        for role, is_in in zip([source, target], [source_in, target_in]):
            condition = flows[f'{role}_{level}'].isin([area])
            if not is_in: condition = ~condition
            conditions.append(condition)
        new_flows = flows[np.bitwise_and.reduce(conditions)]
        amount = kg_to_unit(new_flows['Gewicht_KG'].sum(), unit=unit)
        return amount


def get_classification_graphs(df, source=None,
                              level=None, area=None, klass=None, unit='kg'):
    """
    Create graphs based on ontology classifications
    for LMA & CBS data
    """
    # categories = {
    #     'chains': [
    #         'primair',
    #         'secundair',
    #         'tertiair',
    #         'quaternair',
    #         'Onbekend'
    #     ],
    #     'agendas': [
    #         'BiomassaVoedselTransitieAgenda',
    #         'MaakindustrieTransitieAgenda',
    #         'BouwTransitieAgenda&MaakindustrieTransitieAgenda',
    #         'BouwTransitieAgenda&ConsumptiegoederenTransitieAgenda',
    #         'ConsumptiegoederenTransitieAgenda&MaakindustrieTransitieAgenda',
    #         'ConsumptiegoederenTransitieAgenda',
    #         'NonSpecifiekTransitieAgenda',
    #         'ConsumptiegoederenTransitieAgenda&NonSpecifiekTransitieAgenda',
    #         'BiomassaVoedselTransitieAgenda&KunststoffenTransitieAgenda',
    #         'KunststoffenTransitieAgenda',
    #         'ConsumptiegoederenTransitieAgenda&KunststoffenTransitieAgenda',
    #         'BouwTransitieAgenda',
    #         'BiomassaVoedselTransitieAgenda&BouwTransitieAgenda'
    #         # 'Afval',
    #         # 'Afval&Consumptiegoederen',
    #         # 'Afval&Consumptiegoederen&Textiel',
    #         # 'Bouw',
    #         # 'Bouw&Consumptiegoederen',
    #         # 'Consumptiegoederen',
    #         # 'Consumptiegoederen&Eigen organisatie',
    #         # 'Consumptiegoederen&Overig',
    #         # 'Consumptiegoederen&Textiel',
    #         # 'Eigen Organisatie',
    #         # 'Overig',
    #         # 'Overig&Textiel',
    #         # 'Textiel',
    #         # 'Voedsel',
    #     ]
    # }

    groupby = []

    flows = df.copy()
    if source is not None:
        # filter source in areas
        if isinstance(area, str):
            in_area = flows[f'{source}_{level}'] == area
        elif isinstance(area, list):
            in_area = flows[f'{source}_{level}'].isin(area)
        flows = flows[in_area]
        flows = flows.rename(columns={source: 'source'})

        groupby.append(f'{source}_{level}',)

    # groupby: source, materials
    groupby.extend([
        klass,
        'Gewicht_KG'
    ])
    groups = flows[groupby].groupby(groupby[:-1]).sum().reset_index()

    # specify categories
    cats = sorted(flows[klass].drop_duplicates().to_list())

    # get results for categories
    values = []
    for cat in cats:
        row = groups[groups[klass] == cat]
        value = row['Gewicht_KG'].values[0] if len(row) else 0
        values.append(kg_to_unit(value, unit=unit))
    cats = [format_name(cat) for cat in cats]

    return {
        "name": ','.join(area) if isinstance(area, list) else area,
        klass: cats,
        "values": values,
        "unit": unit
    }


def build_nested(tree_list):
    """
    create nested dict from list
    """
    if tree_list:
        return {tree_list[0]: build_nested(tree_list[1:])}
    return {}


def merge_nested(add, ref):
    """
    merge nested dicts
    add: dict to be added
    ref: original dict
    """
    res = {}
    for key in ref.keys():
        if type(add.get(key, None)) == dict:
            res[key] = merge_nested(add[key], ref[key])
        else:
            res[key] = add.get(key, ref[key])
    for key in add.keys():
        res[key] = res.get(key, add[key])
    return res


def search_nested(key, dic):
    """
    search key in nested dict
    """
    if key in dic: return dic[key]
    for v in dic.values():
        if isinstance(v, dict):
            a = search_nested(key, v)
            if a is not None: return a
    return None


def update_nested(dic, key, value):
    """
    update key in nested dict
    """
    for k, v in dic.items():
        if key == k:
            dic[k] = value
        elif isinstance(v, dict):
            update_nested(v, key, value)


def update_tree(tree, dic, extra):
    for key in dic.keys():
        item = {
            "code": key,
            **extra[key]  # add extra info here
        }
        if isinstance(dic[key], dict):
            item["children"] = []
            item = update_tree(item, dic[key], extra)
        tree["count"] = tree.get("count", 0) + 1
        tree.setdefault("children", []).append(item)
    return tree


def flatten_nested(dic, keys=set()):
    """
    get all keys of nested dict
    """
    for key in dic.keys():
        keys.add(key)
        if isinstance(dic[key], dict):
            flatten_nested(dic[key], keys=keys)
    return sorted(list(keys))


def get_hierarchy(df):
    """
    Get material hierarchy & amounts for lowest levels
    """
    sums = {}  # contains amounts for lowest levels
    hierarchy = {}  # level hierarchy
    for idx, row in df.iterrows():
        # split materials
        materials = row['materials'].split('&')

        # retrieve material intersection
        # to define levels
        levels = materials[0].split(',')
        for material in materials[1:]:
            search = material.split(',')
            new = []
            for item in levels:
                if item in search:
                    new.append(item)
            levels = new
        if len(materials) > 1:
            value = f'{levels[-1]} (gemengd)' if len(levels) else 'Gemengd'
            levels.append(value)
        levels = [format_name(lvl) for lvl in levels]

        # convert into hierarchy
        tree = build_nested(levels)

        # merge with existent hierarchy
        hierarchy = merge_nested(tree, hierarchy)

        # save amount for lowest level
        sums[levels[-1]] = sums.get(levels[-1], 0) + row['Gewicht_KG']

    # populate hierarchy with amounts
    hierarchy = {"Totaal": hierarchy}
    for material in sums.keys():
        obj = search_nested(material, hierarchy)
        if not len(obj):
            update_nested(hierarchy, material, sums[material])
        else:
            obj = search_nested(material, hierarchy)
            obj[f'{material} (andere)'] = sums[material]
            update_nested(hierarchy, material, obj)

    return hierarchy


def close_sankey_sums(nivo, dic, sums):
    """
    Close sums for sankey nodes
    """
    for key in dic.keys():
        nivo['nodes'].add(key)
        if isinstance(dic[key], dict):
            children = dic[key].keys()
            sums[key] = 0
            for child in children:
                nivo['links'].add((key, child))
                close_sankey_sums(nivo, dic[key], sums)
                sums[key] += sums[child]
        else:
            sums[key] = dic[key]
    return nivo, sums


def get_sankey(hierarchy, unit='kg'):
    """
    Convert material hierarchy to nivo sankey
    """
    nivo = {
        'nodes': set(),
        'links': set()
    }
    nivo, sums = close_sankey_sums(nivo, hierarchy, {})
    nivo['nodes'] = [{
        'id': node
    } for node in nivo['nodes']]
    nivo['links'] = [{
        'source': source,
        'target': target
    } for source, target in nivo['links']]
    for link in nivo['links']:
        link['value'] = kg_to_unit(sums[link['target']], unit=unit)
        link['unit'] = unit
        # print(f'{link["source"]}[{sums[link["target"]]}]{link["target"]}')

    return nivo, sums


def get_material_sankey(df, source=None,
                        level=None, area=None, unit='kg'):
    """
    Create material sankey based on
    material ontology for LMA data
    """
    groupby = []

    flows = df.copy()
    if source is not None:
        # filter source in areas
        if isinstance(area, str):
            in_area = flows[f'{source}_{level}'] == area
        elif isinstance(area, list):
            in_area = flows[f'{source}_{level}'].isin(area)
        flows = flows[in_area]
        flows = flows.rename(columns={source: 'source'})

        groupby.append(f'{source}_{level}', )

    # groupby: source, materials
    groupby.extend([
        'materials',
        'Gewicht_KG'
    ])
    groups = flows[groupby].groupby(groupby[:-1]).sum().reset_index()

    # get material hierarchy
    hierarchy = get_hierarchy(groups)

    # convert hierarchy to nivo sankey
    sankey, sums = get_sankey(hierarchy, unit=unit)
    data = {
        "name": ','.join(area) if isinstance(area, list) else area,
        "materials": sankey,
    }

    return data, hierarchy, sums

