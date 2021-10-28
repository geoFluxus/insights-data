import pandas as pd
import geopandas as gpd
import variables as var
import itertools
import numpy as np
import json
from operator import itemgetter
from itertools import groupby
import _make_iterencode


INPUTS = [
    {
        'year': 2019,
        'period': 'year', # year
    },
    # {
    #     'year': 2021,
    #     'period': 'quarter',  # quarter
    #     'periods': [1, 2]
    # }
]

AREAS = {}

ROLES = {
    'Ontvangst': {
        'source': 'Herkomst',
        'target': 'Verwerker',
        'activity': 'Ontdoener'
    },
    'Afgifte': {
        'source': 'EerstAfnemer',
        'target': 'Verwerker',
        'activity': 'EerstAfnemer'
    }
}

DATA = {}

# COROPs in province
COROPS = [
    'Utrecht'
]


def import_areas():
    # municipalities
    gemeenten = gpd.read_file('data/areas/gemeenten.shp')
    gemeenten['centroid'] = gemeenten['geometry'].centroid
    # gemeenten['centroid'] = gemeenten['geometry'].to_crs(epsg=3857).centroid.to_crs(epsg=4326)
    AREAS['Gemeente'] = gemeenten

    # provinces
    provincies = gpd.read_file('data/areas/provincies.shp')
    provincies['centroid'] = provincies['geometry'].centroid
    # provincies['centroid'] = provincies['geometry'].to_crs(epsg=3857).centroid.to_crs(epsg=4326)
    AREAS['Provincie'] = provincies

    # continents
    continents = gpd.read_file('data/areas/continents.shp')
    continents['centroid'] = continents['geometry'].centroid
    # continents['centroid'] = continents['geometry'].to_crs(epsg=3857).centroid.to_crs(epsg=4326)
    AREAS['Continent'] = continents

    # countries
    countries = gpd.read_file('data/areas/countries.shp')
    countries['country_nl'] = countries['country_nl'].str.upper()
    countries = pd.merge(countries, continents[['cont_en', 'cont_nl']], how='left', on='cont_en')
    AREAS['Country'] = countries

    # list of province municipalities
    provincie_gemeenten = gemeenten[gemeenten['parent'] == var.PROVINCE]['name'].to_list()

    return provincie_gemeenten


def group_flows(df, period='year'):
    """
    Assign flows to year periods
    """
    durations = {
        'quarter': 3,
        'year': 12
    }
    per_months = durations[period]

    months = list(range(1, 13))
    periods = [
        months[i: i + per_months] for i in range(0, len(months), per_months)
    ]
    for idx, period in enumerate(periods):
        df.loc[df['MeldPeriodeMAAND'].isin(period), 'Periode'] = idx + 1
    df['Periode'] = df['Periode'].astype(int)
    return df


def add_areas(flows, areas=None, role=None, admin_level=None):
    """
    Add administrative areas to roles
    flows: flow dataframe -> DataFrame
    areas: area polygons -> GeoDataFrame
    role: role to assign areas -> string
    admin_level: administrative level of areas -> string
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


def to_flowmap(df, level=None, extra=[]):
    level_areas = AREAS[level]
    continents = AREAS['Continent']

    for field, areas in zip(['name', 'cont_nl'], [level_areas, continents]):
        for node in ['source', 'target']:
            columns = list(df.columns)
            df = pd.merge(df, areas, how='left', left_on=node, right_on=field)
            if f'{node}_centroid' not in columns:
                df[f'{node}_centroid'] = df['centroid']
                columns.append(f'{node}_centroid')
            else:
                df.loc[df[f'{node}_centroid'].isnull(), f'{node}'] = df['cont_nl']
                df.loc[df[f'{node}_centroid'].isnull(), f'{node}_centroid'] = df['centroid']
            df = df[columns]

    flows = []
    for idx, row in df.iterrows():
        flow = {
            'amount': row['Gewicht_KG'],
            'source': {
                'name': row['source'],
                'lon': row['source_centroid'].x,
                'lat': row['source_centroid'].y
            },
            'target': {
                'name': row['target'],
                'lon': row['target_centroid'].x,
                'lat': row['target_centroid'].y
            }
        }
        for ex in extra:
            flow[ex] = row[ex]
        flows.append(flow)

    return flows


def get_flows(df,
              period=None,
              source=None, source_in=None,
              target=None, target_in=None,
              level=None, areas=[],
              groupby=[], rename={}):
    # extra fields for json
    extra = list(rename.values())

    # filter dataframe
    conditions = []
    for node, node_in in zip([source, target], [source_in, target_in]):
        if node_in is None: continue
        condition = df[f'{node}_{level}'].isin(areas)
        if not node_in: condition = ~condition
        conditions.append(condition)
    if period: conditions.append(df['Periode'] == period)
    flows = df[np.bitwise_and.reduce(conditions)].copy() if len(conditions) else df.copy()

    # groupby columns
    _groupby = groupby.copy()
    _groupby.extend([
        'source',
        'target',
        'Gewicht_KG'
    ])

    columns = {
        f'{source}_{level}': 'source',
        f'{target}_{level}': 'target',
    }

    # split to flows with source/target in/out Netherlands
    for node in [source, target]:
        condition = (flows[f'{node}_Land'] == 'NEDERLAND') |\
                    (flows[f'{node}_Land'] == 'NAN')
        flows.loc[condition, f'{node}_in'] = True
        flows.loc[~condition, f'{node}_in'] = False
        flows.loc[flows[f'{node}_in'] == True, columns[f'{node}_{level}']] = flows[f'{node}_{level}']
        flows.loc[flows[f'{node}_in'] == False, columns[f'{node}_{level}']] = flows[f'{node}_Continent']

    # aggregate
    groups = flows[_groupby].groupby(_groupby[:-1]).sum().reset_index()
    groups = groups.rename(columns=rename)

    return to_flowmap(groups, level=level, extra=extra)


def build_tree(tree_list):
    if tree_list:
        return {tree_list[0]: build_tree(tree_list[1:])}
    return {}

def merge(add, ref):
    res = {}
    for key in ref.keys():
        if type(add.get(key, None)) == dict:
            res[key] = merge(add[key], ref[key])
        else:
            res[key] = add.get(key, ref[key])
    for key in add.keys():
        res[key] = res.get(key, add[key])
    return res

def search_nested(key, dic):
    if key in dic: return dic[key]
    for v in dic.values():
        if isinstance(v, dict):
            a = search_nested(key, v)
            if a is not None: return a
    return None

def update_nested(dic, key, value):
    for k, v in dic.items():
        if key == k:
            dic[k] = value
        elif isinstance(v, dict):
            update_nested(v, key, value)

def nivo_sunburst(nivo, dic):
    for key in dic.keys():
        name = key
        item = {
            "name": name,
        }
        if isinstance(dic[key], dict):
            item["children"] = []
            item = nivo_sunburst(item, dic[key])
        else:
            item["loc"] = dic[key]
        nivo["children"].append(item)
    return nivo


def get_sunburst(hierarchy):
    nivo = {
        "name": "nivo",
        "children": []
    }
    return nivo_sunburst(nivo, hierarchy)


def nivo_sankey(nivo, dic, sums):
    for key in dic.keys():
        nivo['nodes'].add(key)
        if isinstance(dic[key], dict):
            children = dic[key].keys()
            sums[key] = 0
            for child in children:
                nivo['links'].add((key, child))
                nivo_sankey(nivo, dic[key], sums)
                sums[key] += sums[child]
        else:
            sums[key] = dic[key]
    return nivo, sums


def get_sankey(hierarchy):
    # create nivo sankey
    # MISSING SINGLE NODES: Mixed, Unknown
    nivo = {
        'nodes': set(),
        'links': set()
    }
    nivo, sums = nivo_sankey(nivo, hierarchy, {})
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


def get_hierarchy(df):
    sums = {}  # contains amounts for all levels
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
        tree = build_tree(levels)

        # merge with existent hierarchy
        hierarchy = merge(tree, hierarchy)

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


def get_material_use(df, period=None, source=None,
                     level=None, areas=None):
    # filter source in areas
    flows = df[df[f'{source}_{level}'].isin(areas)]
    flows = flows.rename(columns={source: 'source'})

    # filter by period
    if period: flows = flows[flows['Periode'] == period]

    # groupby: source, materials
    groupby = [
        f'{source}_{level}',
        'materials',
        'Gewicht_KG'
    ]
    groups = flows[groupby].groupby(groupby[:-1]).sum().reset_index()

    # sunbursts = []
    sankeys = []
    for area in areas:
        df = groups[groups[f'{source}_{level}'] == area]
        hierarchy = get_hierarchy(df)

        sankey, sums = get_sankey(hierarchy)
        sankeys.append({
            "name": area,
            "materials": sankey
        })

        # sunbursts.append({
        #     "name": area,
        #     "materials": get_sunburst(hierarchy)
        # })

    return sankeys, hierarchy, sums


def get_classification_graphs(df, period=None, source=None,
                              level=None, areas=None, klass=None):
    # filter source in areas
    flows = df[df[f'{source}_{level}'].isin(areas)]
    flows = flows.rename(columns={source: 'source'})

    # filter by period
    if period: flows = flows[flows['Periode'] == period]

    # groupby: source, materials
    groupby = [
        f'{source}_{level}',
        klass,
        'Gewicht_KG'
    ]
    groups = flows[groupby].groupby(groupby[:-1]).sum().reset_index()

    results = []
    for area in areas:
        select = groups[groups[f'{source}_{level}'] == area]

        collection = {}
        for idx, row in select.iterrows():
            # classifs = row[klass].split('&')
            # for classif in classifs:
            collection[row[klass]] = collection.get(row[klass], 0) + row['Gewicht_KG']

        classifs, values = [],  []
        for classif, value in collection.items():
            classifs.append(classif)
            values.append(round(value / 10**9, 2))  # kg -> Mt

        results.append({
            "name": area,
            klass: classifs,
            "values": {
                "weight": {
                    "value": values,
                    "unit": "Mt"
                }
            }
        })

    return results


def add_classification(df, classif, name=None,
                       left_on=None, right_on=None):
    classif[right_on] = classif[right_on].astype(str).str.zfill(6)
    df[left_on] = df[left_on].astype(str).str.zfill(6)
    df = pd.merge(df, classif, how='left', left_on=left_on, right_on=right_on)
    df.loc[df[name].isnull(), name] = 'Unknown'
    return df


if __name__ == "__main__":
    # import areas
    print('Import areas...')
    provincie_gemeenten = import_areas()
    provincie = var.PROVINCE

    # import ewc classifications
    classifs = {}
    for classif in ['agendas', 'materials']:
        classifs[classif] = pd.read_csv(f'./data/materials/ewc_{classif}.csv', low_memory=False, sep=';')

    tree = {}
    for input in INPUTS:
        year = input['year']
        period = input['period']
        periods = input.get('periods', [None])

        print(f'YEAR: {year}')

        # iterate all flow types
        for typ in ['Ontvangst', 'Afgifte']:
            # import file
            print('')
            print(f'Import {typ}....')
            path = f'../../../../../media/geofluxus/DATA/national/{var.PROVINCE.lower()}/processed'
            filename = f'{path}/{typ.lower()}_{var.PROVINCE.lower()}_{year}.csv'
            df = pd.read_csv(filename, low_memory=False)

            # group flows to periods
            print('Split to periods...')
            df = group_flows(df, period=period)

            # add areas to roles
            print('Add areas to roles...')
            source = ROLES[typ]['source']  # source role
            target = ROLES[typ]['target']  # target role
            for role, level in itertools.product([source, target], ['Provincie']):
                areas = AREAS[level]
                df = add_areas(df, areas=areas, role=role, admin_level=level)

            # add classifications
            for name, classif in classifs.items():
                df = add_classification(df, classif, name=name,
                                        left_on='EuralCode',
                                        right_on='ewc')

            prefixes = {
                'Provincie': 'province',
                'Gemeente': 'municipality',
                'Ontvangst': 'primary',
                'Afgifte': 'secondary',
            }

            # analyse on provincial & municipal level
            print('Analyse...')
            for level in ['Provincie', 'Gemeente']:
                areas = [provincie] if level == 'Provincie' else provincie_gemeenten

                prefix = f'{prefixes[level]}\t{prefixes[typ]} waste'

                for p in periods:
                    suffix = f'{year}'
                    if p: suffix = f'{suffix}-{period[0].upper()}{p}'

                    # only on province level & primary waste
                    if prefixes[level] == 'province' and prefixes[typ] == 'primary':
                        # material use
                        DATA[f'{prefix}\tsankey\t{suffix}'], hierarchy, sums =\
                            get_material_use(df,
                                             period=p,
                                             source=source,
                                             level=level, areas=areas)
                        tree['EWC'] = {
                            'hierarchy': hierarchy,
                            'sums': sums
                        }

    #                     # transition agendas
    #                     DATA[f'{prefix}\ttransition_agendas\t{suffix}'] =\
    #                         get_classification_graphs(df,
    #                                                   period=p,
    #                                                   source=source,
    #                                                   level=level, areas=areas,
    #                                                   klass='agendas')

    # CBS DATA
    # stromen -> million kg
    path = './data/cbs/Tabel Regionale stromen 2015-2019.csv'
    df = pd.read_csv(path, low_memory=False, sep=';')
    df['Gewicht_KG'] = round(df['Brutogew'] * 10**6)
    df['Gewicht_KG'] = df['Gewicht_KG'].astype(int)

    # filter by year & COROPS
    df = df[(df['Jaar'] == year) & (df['COROP_naam'].isin(COROPS))]
    stromen = [
        'Aanbod_eigen_regio',
        'Distributie',
        'Doorvoer',
        'Invoer_internationaal',
        'Invoer_regionaal',
        'Uitvoer_internationaal',
        'Uitvoer_regionaal'
    ]

    # import cbs classifications
    classifs = {}
    for classif in ['agendas', 'materials']:
        classifs[classif] = pd.read_csv(f'./data/materials/cbs_{classif}.csv', low_memory=False, sep=';')

    # add classifications
    for name, classif in classifs.items():
        df = add_classification(df, classif, name=name,
                                left_on='Goederengroep_nr',
                                right_on='cbs')

    # # TRANSITION AGENDAS
    # # groupby: source, materials
    # groupby = [
    #     'agendas',
    #     'Brutogew'
    # ]
    # groups = df[groupby].groupby(groupby[:-1]).sum().reset_index()
    #
    # results = []
    #
    # collection = {}
    # for idx, row in groups.iterrows():
    #     collection[row['agendas']] = collection.get(row['agendas'], 0) + row['Brutogew']
    #
    # classifs, values = [], []
    # for classif, value in collection.items():
    #     classifs.append(classif)
    #     values.append(round(value / 10**3, 2))  # ml kg -> Mt
    #
    # results.append({
    #     "name": var.PROVINCE,
    #     "agendas": classifs,
    #     "values": {
    #         "weight": {
    #             "value": values,
    #             "unit": "Mt"
    #         }
    #     }
    # })
    # prefix = 'province\tmaterial'
    # DATA[f'{prefix}\tagendas\t{suffix}'] = results

    # SANKEY
    sankey, sums = get_sankey(get_hierarchy(df))
    # DATA[f'province\tmaterials\tsankey\t{year}'] = [{
    #     "name": var.PROVINCE,
    #     "materials": sankey
    # }]
    tree['NST'] = {
        'hierarchy': get_hierarchy(df),
        'sums': sums
    }
    print(json.dumps(tree, indent=4))

    # merge all to tree
    hierarchy = {}
    total = {}
    for typ, item in tree.items():
        hierarchy = merge(item['hierarchy'], hierarchy)
        # for key, value in item['sums'].items():
        #     total[key] = total.get(key, 0) + value

    def flatten_hierarchy(dic, keys=set()):
        """
        recover all hierarchy levels
        """
        for key in dic.keys():
            keys.add(key)
            if isinstance(dic[key], dict):
                flatten_hierarchy(dic[key], keys=keys)
        return sorted(list(keys))

    sums = {}
    for typ, item in tree.items():
        for k in flatten_hierarchy(hierarchy):
            sums.setdefault(k, []).append({
                "type": typ,
                "value": item['sums'].get(k, 0)
                # "value": round(item['sums'].get(k, 0) / total[k] * 100)
            })

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

    print(json.dumps(update_tree({}, hierarchy)))

    # # GRAPHS
    # with open('test/materials.json', 'w') as outfile:
    #     # preprocess
    #     results = {}
    #     for key, items in DATA.items():
    #         level, type, field, period = key.split('\t')
    #         for item in items:
    #             item['level'] = level
    #             item['period'] = period
    #             item['type'] = type
    #             results.setdefault(field, []).append(item)
    #
    #     json.encoder._make_iterencode = _make_iterencode._make_iterencode
    #     indent = (2, None)
    #     json.dump(results, outfile, indent=indent)