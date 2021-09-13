import pandas as pd
import geopandas as gpd
import variables as var
import itertools
import numpy as np
import json
from shapely import wkt
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

MAP = {} # map data

DATA = {}

AREAS = {}

NETWORK = {}

ACTIVITIES = None

PROCESSES = None


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


def add_identifiers(df, type=None):
    if type == 'Ontvangst':
        df['origin'] = df['Ontdoener'] + " " + df['Herkomst_Postcode']
    else:
        df['origin'] = df['EerstAfnemer'] + " " + df['EerstAfnemer_Postcode']
    df['origin'] = df['origin'].str.replace(" ", "_")
    df['destination'] = df['Verwerker'] + " " + df['Verwerker_Postcode'] + " " + df["VerwerkingsmethodeCode"]
    df['destination'] = df['destination'].str.replace(" ", "_")

    return df


def add_routings(flows):
    # join with routings
    flows = pd.merge(flows, routings, how='left', on=['origin', 'destination'])
    unmatched = len(flows[flows['origin'].isnull()])
    if unmatched:
        print(f'No routings for {unmatched.index} flows...')

    # TODO: fill with random numbers (Aantal_vrachten)
    vehicles = pd.read_excel('data/network/vehicle.xlsx')
    flows['tn'] = flows['Gewicht_KG'] / 10 ** 3
    flows['average'] = flows['tn'] / flows['Aantal_vrachten']

    # TODO: compute CO2 emissions
    for idx, row in vehicles.iterrows():
        min, max = row['min'], row['max']
        condition = (flows['average'] >= min) & (flows['average'] < max)
        flows.loc[condition, 'grams per tonne kilometer'] = row['co2']
    flows['co2'] = flows['grams per tonne kilometer'] * flows['tn'] * flows['distance'] / 10**3

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


def get_activities(df, period=None, source=None,
                   level=None, areas=None):
    activities = ACTIVITIES['code'].to_list()
    activity_descriptions = ACTIVITIES['name'].to_list()
    processes = PROCESSES['code'].to_list()
    process_descriptions = PROCESSES['name'].to_list()
    results = []

    # filter source in areas
    flows = df[df[f'{source}_{level}'].isin(areas)]
    flows = flows.rename(columns={source: 'source'})

    # filter by period
    if period: flows = flows[flows['Periode'] == period]

    # group by ACTIVITY
    groups = flows.groupby([
        f'{source}_{level}',
        f'{source}_AG'
    ])\
        .agg({'Gewicht_KG': [
        'count',
        'sum'
    ]}).reset_index()
    groups.columns = ['_'.join(col).strip() for col in groups.columns.values]
    groups = groups.rename(columns={
        f'{source}_{level}_': 'area',
        f'{source}_AG_': 'activity',
        'Gewicht_KG_count': 'count',
        'Gewicht_KG_sum': 'sum'
    })
    # add missing values
    for area in areas:
        for activity in activities:
            search = groups[
                (groups['area'] == area) &
                (groups['activity'] == activity)
            ]
            if len(search) == 0:
                groups.loc[len(groups)] = [area, activity, 0, 0]
    groups = groups.sort_values(by=['area', 'activity'])

    # write results
    # TODO: Add activity descriptions in Dutch
    for area in areas:
        area_results = groups[groups['area'] == area]
        weight, count = [], []
        for idx, item in area_results.iterrows():
            weight.append(round(item['sum'] / 10**6, 2))
            count.append(item['count'])
        results.append({
            'name': item['area'],
            'activity': activity_descriptions,
            'values': {
                'weight': {
                    'value': weight,
                    'unit': 'ktn'
                },
                'count': {
                    'value': count,
                    'unit': None
                }
            }
        })

    # group by ACTIVITY & PROCESS
    # TODO: have a constant grid of activities & processes -> add missing values
    groups = flows.groupby([
        f'{source}_{level}',
        f'{source}_AG',
        'VerwerkingsGroep'
    ])\
        .agg({'Gewicht_KG': [
        'sum'
    ]}).reset_index()
    groups.columns = ['_'.join(col).strip() for col in groups.columns.values]
    groups = groups.rename(columns={
        f'{source}_{level}_': 'area',
        f'{source}_AG_': 'activity',
        'VerwerkingsGroep_': 'process',
        'Gewicht_KG_sum': 'sum'
    })
    # add missing values
    for area in areas:
        for activity in activities:
            for process in processes:
                search = groups[
                    (groups['area'] == area) &
                    (groups['activity'] == activity) &
                    (groups['process'] == process)
                ]
                if len(search) == 0:
                    groups.loc[len(groups)] = [area, activity, process, 0]
    groups = groups.sort_values(by=['area', 'activity', 'process'])

    # write results
    for area in areas:
        weight = []
        for activity in activities:
            area_results = groups[
                (groups['area'] == area) &
                (groups['activity'] == activity)
            ]
            activity_weight = []
            for idx, item in area_results.iterrows():
                activity_weight.append(round(item['sum'] / 10**6, 2))
            weight.append(activity_weight)
        results.append({
            'name': item['area'],
            'activity': activity_descriptions,
            'process': process_descriptions,
            'values': {
                'weight': {
                    'value': weight,
                    'unit': 'ktn'
                }
            }
        })

    return results


def get_emissions(df,
                period=None,
                source=None, source_in=True,
                target=None, target_in=True,
                level=None, areas=[]):
    results = []

    # filter dataframe
    conditions = []
    for node, node_in in zip([source, target], [source_in, target_in]):
        condition = df[f'{node}_{level}'].isin(areas)
        if not node_in: condition = ~condition
        conditions.append(condition)
    if period: conditions.append(df['Periode'] == period)
    flows = df[np.bitwise_and.reduce(conditions)]

    # totals
    area = f'{source}_{level}' if source_in else f'{target}_{level}'
    groupby = [
        area,
        'co2'
    ]
    groups = flows[groupby].groupby(groupby[:-1]).sum().reset_index()
    groups = groups.rename(columns={area: 'area'})

    # add missing values
    for area in areas:
        search = groups[groups['area'] == area]
        if len(search) == 0:
            groups.loc[len(groups)] = [area, 0]
    groups = groups.sort_values(by=['area'])

    # write results
    for idx, item in groups.iterrows():
        results.append({
            'name': item['area'],
            'values': {
                'weight': {
                    'value': round(item['co2'] / 10**9, 2),
                    'unit': 'ktn'
                }
            }
        })

    return results


def get_network(df):
    # distribute along network
    ways = {}
    for idx, flow in df.iterrows():
        seq, amount = flow['seq'], flow['co2']
        if type(seq) == str:
            seq = [id for id in seq.split('@')]
            if np.isnan(amount): amount = 0
            for id in seq:
                if id in ways:
                    ways[id] += amount
                else:
                    ways[id] = amount

    return ways


def group_activities(results):
    keys = [key for key in results.keys() if 'activities' in key]
    types = [
        typ.replace('_activities', '').replace('_', ' ')
        for typ in keys
    ]
    datasets = [results.pop(key) for key in keys]
    for typ, dataset in zip(types, datasets):
        for item in dataset:
            process = item.get('process', None)
            key = 'activities' if process is None else 'process_per_activity'
            item['type'] = typ
            results.setdefault(key, []).append(item)

    return results


def group_emissions(results):
    # emissions_co2
    keys = [key for key in results.keys() if 'co2' in key]
    types = [
        typ.replace('_co2', '').replace('_', ' ')
        for typ in keys
    ]
    datasets = [results.pop(key) for key in keys]
    for item in datasets[0]:
        weight = item["values"]["weight"]
        values, unit = [weight["value"]], weight["unit"]
        for d in datasets[1:]:
            other = next(
                res for res in d
                if all(res[prop] == item[prop] for prop in ['name', 'level', 'period'])
            )
            weight = other["values"]["weight"]
            values.append(weight["value"])
        results.setdefault('emissions_co2', []).append({
            "name": item["name"],
            "type": types,
            "values": {
                "weight": {
                    "value": values,
                    "unit": unit
                }
            },
            "level": item["level"],
            "period": item["period"]
        })

    return results


def export():
    # GRAPHS
    with open('test/overview.json', 'w') as outfile:
        # preprocess
        results = {}
        for key, items in DATA.items():
            level, field, period = key.split('\t')
            for item in items:
                item['level'] = level
                item['period'] = period
                results.setdefault(field, []).append(item)

        # group activities
        results = group_activities(results)

        # group emissions
        results = group_emissions(results)

        # export
        json.encoder._make_iterencode = _make_iterencode._make_iterencode
        indent = (2, None)
        json.dump(results, outfile, indent=indent)

    # NETWORK MAP
    data = MAP.pop('transport', {})
    # add ontvangst & afgifte
    from collections import Counter
    ways = Counter()
    for key in data.keys():
        ways.update(data[key])
    # load to segments
    results = []
    for way in NETWORK.items():
        id, geometry = way
        id = str(id)
        if id not in ways: ways[id] = 0
        results.append({
            'id': id,
            'geometry': geometry,
            'amount': round(ways[id] / 10**6, 2)  # grams -> tn
        })
    with open('test/overview_co2_network.json', 'w') as outfile:
        json.dump(results, outfile, indent=4)

    # FLOWMAPS
    for section, data in MAP.items():
        with open(f'test/overview_{section}_flowmap.json', 'w') as outfile:
            results = []
            for key, items in data.items():
                level, field, period = key.split('\t')
                type = field.replace('_', ' ')
                for item in items:
                    item['period'] = period
                    item['type'] = type
                    results.append(item)
            json.dump(results, outfile, indent=4)


if __name__ == "__main__":
    # import areas
    print('Import areas...')
    provincie_gemeenten = import_areas()
    provincie = var.PROVINCE

    # import routings
    print('Import routings...')
    routings = pd.read_csv('data/network/routings.csv', low_memory=False, sep=';')
    # routings['distance'] = gpd.GeoSeries.from_wkt(routings[routings['wkt'].notnull()]['wkt'])\
    #                                     .set_crs('epsg:4326')\
    #                                     .to_crs('epsg:3857')\
    #                                     .length
    # routings.to_csv('data/network/routings.csv', index=False, sep=';')

    # import activities & processes
    ACTIVITIES = pd.read_excel('data/flows/activitygroup.xlsx')
    PROCESSES = pd.read_excel('data/flows/processgroup.xlsx')

    # import network
    with open('data/network/network.geojson') as f:
        geojson = json.load(f)
        for feat in geojson['features']:
            id = str(feat['properties']['id'])
            NETWORK[id] = feat['geometry']

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

            # add activity names
            columns = list(df.columns)
            df = pd.merge(df, ACTIVITIES, how='left', left_on=f'{activity}_AG', right_on='code')
            df = df.rename(columns={'name': 'Activity'})
            columns.append('Activity')
            df = df[columns]

            # add process names
            columns = list(df.columns)
            df = pd.merge(df, ACTIVITIES, how='left', left_on='VerwerkingsGroep', right_on='code')
            df = df.rename(columns={'name': 'Process'})
            columns.append('Process')
            df = df[columns]

            # add identifiers
            print('Add identifiers to flows...')
            df = add_identifiers(df, type=typ)

            # add routings
            print('Add routings to flows...')
            df = add_routings(df)

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

                prefix = f'{prefixes[level]}\t{prefixes[typ]}_waste'

                for p in periods:
                    suffix = f'{year}'
                    if p: suffix = f'{suffix}-{period[0].upper()}{p}'

                    # RESOURCES
                    if prefixes[level] == 'province':
                        # import (source out, target in) -> FLOWMAP
                        MAP.setdefault('resources', {})[f'{prefix}_import\t{suffix}'] = \
                            get_flows(df,
                                      period=p,
                                      source=source, source_in=False,
                                      target=target, target_in=True,
                                      level=level, areas=areas)

                        # export (source in, target out) -> FLOWMAP
                        MAP.setdefault('resources', {})[f'{prefix}_export\t{suffix}'] = \
                            get_flows(df,
                                      period=p,
                                      source=source, source_in=True,
                                      target=target, target_in=False,
                                      level=level, areas=areas)

                    # ECONOMIC ACTIVITIES
                    if prefixes[typ] == 'primary':
                        # GRAPHS
                        if prefixes[level] == 'province':
                            DATA[f'{prefix}_activities\t{suffix}'] = \
                                get_activities(df,
                                               period=p,
                                               source=activity,
                                               level=level, areas=areas)

                        # FLOWMAPS
                        if prefixes[level] == 'municipality':
                            # economic activities (Herkomst in) -> FLOWMAP
                            MAP.setdefault('activities', {})[f'{prefix}_activity\t{suffix}'] = \
                                get_flows(df,
                                          period=p,
                                          source=source, source_in=True,
                                          target=target,
                                          level=level, areas=areas,
                                          groupby=['Activity'], rename={'Activity': 'activity'})

                            # waste processes (Herkomst in) -> FLOWMAP
                            MAP.setdefault('processes', {})[f'{prefix}_process\t{suffix}'] = \
                                get_flows(df,
                                          period=p,
                                          source=source, source_in=True,
                                          target=target,
                                          level=level, areas=areas,
                                          groupby=['Process'],
                                          rename={'Process': 'process'})

                    # TRANSPORT
                    # GRAPHS
                    if prefixes[level] == 'province':
                        DATA[f'{prefix}_import_co2\t{suffix}'] = \
                            get_emissions(df,
                                        period=p,
                                        source=activity, source_in=False,
                                        target=target, target_in=True,
                                        level=level, areas=areas)

                        DATA[f'{prefix}_export_co2\t{suffix}'] = \
                            get_emissions(df,
                                        period=p,
                                        source=activity, source_in=True,
                                        target=target, target_in=False,
                                        level=level, areas=areas)

            # CO2 NETWORK MAP (all levels)
            MAP.setdefault('transport', {})[f'{prefixes[typ]}_waste\tco2'] = get_network(df)

        print('\n')

    # export analysis
    export()



