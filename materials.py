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
        'year': 2020,
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

MAP = {}


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
        if 'Mixed' in name: name = 'Mixed'
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

    sunbursts = []
    for area in areas:
        select = groups[groups[f'{source}_{level}'] == area]

        sums = {}  # contains amounts for all levels
        hierarchy = {}  # level hierarchy
        for idx, row in select.iterrows():
            # split materials
            materials = row['materials'].split('&')

            # retrieve material intersection
            # to defin levels
            levels = materials[0].split(',')
            for material in materials[1:]:
                search = material.split(',')
                new = []
                for item in levels:
                    if item in search:
                        new.append(item)
                levels = new
            if len(materials) > 1:
                levels.append(','.join(levels) + ',Mixed')

            # convert into hierarchy
            tree = build_tree(levels)

            # merge with existent hierarchy
            hierarchy = merge(tree, hierarchy)

            # save amount for lowest level
            sums[levels[-1]] = sums.get(levels[-1], 0) + row['Gewicht_KG']

        # populate hierarchy with amounts
        for material in sums.keys():
            obj = search_nested(material, hierarchy)
            if not len(obj):
                update_nested(hierarchy, material, sums[material])
            else:
                obj = search_nested(material, hierarchy)
                obj['Other'] = sums[material]
                update_nested(hierarchy, material, obj)

        # create nivo sunburst
        nivo = {
            "name": "nivo",
            "children": []
        }
        sunbursts.append({
            "name": area,
            "materials": nivo_sunburst(nivo, hierarchy)
        })

    return sunbursts


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


def add_classification(df, classif, name=None):
    classif['ewc'] = classif['ewc'].astype(str).str.zfill(6)
    df['EuralCode'] = df['EuralCode'].astype(str).str.zfill(6)
    df = pd.merge(df, classif, how='left', left_on='EuralCode', right_on='ewc')
    df.loc[df[name].isnull(), name] = 'Unknown'
    return df


if __name__ == "__main__":
    # import areas
    print('Import areas...')
    provincie_gemeenten = import_areas()
    provincie = var.PROVINCE

    # import ewc classifications
    classifs = {}
    for classif in ['materials', 'agendas', 'chains']:
        classifs[classif] = pd.read_csv(f'./data/materials/ewc_{classif}.csv', low_memory=False, sep=';')

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
            df['VerwerkingsGroep'] = df['VerwerkingsmethodeCode'].str[0]

            # group flows to periods
            print('Split to periods...')
            df = group_flows(df, period=period)

            # add areas to roles
            print('Add areas to roles...')
            source = ROLES[typ]['source']  # source role
            target = ROLES[typ]['target']  # target role
            activity = ROLES[typ]['activity']  # activity role (ontvangst: 'ontdoener')
            for role, level in itertools.product([source, target], ['Provincie', 'Gemeente']):
                areas = AREAS[level]
                df = add_areas(df, areas=areas, role=role, admin_level=level)
            if typ == 'Ontvangst':
                df = add_areas(df, areas=AREAS['Provincie'], role=activity, admin_level="Provincie")
                df = add_areas(df, areas=AREAS['Gemeente'], role=activity, admin_level="Gemeente")

            # add continents based on countries to roles
            countries = AREAS['Country']
            countries = countries[['country_nl', 'cont_nl']]
            for role in [source, target]:
                columns = list(df.columns)
                df = pd.merge(df, countries, how='left', left_on=f'{role}_Land', right_on='country_nl')
                df[f'{role}_Continent'] = df['cont_nl']
                columns.append(f'{role}_Continent')
                df = df[columns]

            # add classifications
            for name, classif in classifs.items():
                df = add_classification(df, classif, name=name)

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
                        DATA[f'{prefix}\tmaterial_use\t{suffix}'] =\
                            get_material_use(df,
                                             period=p,
                                             source=source,
                                             level=level, areas=areas)

                        # transition agendas
                        DATA[f'{prefix}\ttransition_agendas\t{suffix}'] =\
                            get_classification_graphs(df,
                                                      period=p,
                                                      source=source,
                                                      level=level, areas=areas,
                                                      klass='agendas')

                        # supply chains
                        DATA[f'{prefix}\tsupply_chains\t{suffix}'] = \
                            get_classification_graphs(df,
                                                      period=p,
                                                      source=source,
                                                      level=level, areas=areas,
                                                      klass='chains')

    # GRAPHS
    with open('test/materials.json', 'w') as outfile:
        # preprocess
        results = {}
        for key, items in DATA.items():
            level, type, field, period = key.split('\t')
            for item in items:
                item['level'] = level
                item['period'] = period
                item['type'] = type
                results.setdefault(field, []).append(item)

        json.encoder._make_iterencode = _make_iterencode._make_iterencode
        indent = (2, None)
        json.dump(results, outfile, indent=indent)