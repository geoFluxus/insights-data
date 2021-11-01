import pandas as pd
import geopandas as gpd
import numpy as np
import json
import _make_iterencode


def export_graphs(fil, data=None):
    with open(fil, 'w') as outfile:
        # preprocess
        results = {}
        for key, items in data.items():
            level, type, field, period = key.split('\t')
            for item in items:
                item['level'] = level
                item['period'] = period
                item['type'] = type
                results.setdefault(field, []).append(item)

        json.encoder._make_iterencode = _make_iterencode._make_iterencode
        indent = (2, None)
        json.dump(results, outfile, indent=indent)


def import_areas(level=None):
    """
    import area polygons
    for different administration level
    (municipalities, provinces etc.)
    """
    # load geometries
    areas = gpd.read_file(f'./data/areas/{level}.shp')
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
    flows = gpd.sjoin(flows, areas, how='left', op='within')

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
    classif[right_on] = classif[right_on].astype(str).str.zfill(6)
    df[left_on] = df[left_on].astype(str).str.zfill(6)
    df = pd.merge(df, classif, how='left', left_on=left_on, right_on=right_on)
    df.loc[df[name].isnull(), name] = 'Unknown'
    return df


def compute_sankey_branch(flows,
                          source=None, source_in=True,
                          target=None, target_in=True,
                          level=None, areas=[]):
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
        new_flows = new_flows['Gewicht_KG'].sum() / 10**9  # megatonnes
        print(f'{source} ({binnen[source_in]} {area}) -> {target} ({binnen[target_in]} {area}): {new_flows} Mtn')


def get_classification_graphs(df, source=None,
                              level=None, area=None, klass=None):
    """
    Create graphs based on ontology classifications
    for LMA & CBS data
    """
    categories = {
        'chains': [
            'primair',
            'secundair',
            'tertiair',
            'quartair',
            'unknown'
        ]
    }

    groupby = []

    flows = df.copy()
    if source is not None:
        # filter source in areas
        flows = flows[flows[f'{source}_{level}'].isin([area])]
        flows = flows.rename(columns={source: 'source'})

        groupby.append(f'{source}_{level}',)

    # groupby: source, materials
    groupby.extend([
        klass,
        'Gewicht_KG'
    ])
    groups = flows[groupby].groupby(groupby[:-1]).sum().reset_index()

    # specify categories
    cats = categories.get(klass, None)
    if cats is None:
        cats = groups['agendas'].drop_duplicates().to_list()

    # get results for categories
    results = []
    values = []
    for cat in cats:
        row = groups[groups[klass] == cat]
        value = row['Gewicht_KG'].values[0] if len(row) else 0
        values.append(round(value / 10**9, 2))  # kg -> Mt

    # add to results
    results.append({
        "name": area,
        klass: cats,
        "values": {
            "weight": {
                "value": values,
                "unit": "Mt"
            }
        }
    })

    return results


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
            value = f'{levels[-1]} (Mixed)' if len(levels) else 'Mixed'
            levels.append(value)

        # convert into hierarchy
        tree = build_nested(levels)

        # merge with existent hierarchy
        hierarchy = merge_nested(tree, hierarchy)

        # save amount for lowest level
        sums[levels[-1]] = sums.get(levels[-1], 0) + row['Gewicht_KG']

    # populate hierarchy with amounts
    hierarchy = {"Total": hierarchy}
    for material in sums.keys():
        obj = search_nested(material, hierarchy)
        if not len(obj):
            update_nested(hierarchy, material, sums[material])
        else:
            obj = search_nested(material, hierarchy)
            obj[f'{material} (Other)'] = sums[material]
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


def get_sankey(hierarchy):
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
        link['value'] = sums[link['target']]

    return nivo, sums


def get_material_sankey(df, source=None,
                        level=None, area=None):
    """
    Create material sankey based on
    material ontology for LMA data
    """
    groupby = []

    flows = df.copy()
    if source is not None:
        # filter source in areas
        flows = flows[flows[f'{source}_{level}'].isin([area])]
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
    sankey, sums = get_sankey(hierarchy)

    sankeys = []
    sankeys.append({
        "name": area,
        "materials": sankey
    })

    return sankeys, hierarchy, sums

