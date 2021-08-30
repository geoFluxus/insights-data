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

RESOURCE = {} # resource flow data

DATA = {}

AREAS = {}

NETWORK = {}

ACTIVITIES = None

PROCESSES = None


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


def to_flowmap(df, level=None):
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
        flows.append(flow)

    return flows


def get_flows(df,
              period=None,
              source=None, source_in=True,
              target=None, target_in=True,
              level=None, areas=[]):
    # filter dataframe
    conditions = []
    for node, node_in in zip([source, target], [source_in, target_in]):
        condition = df[f'{node}_{level}'].isin(areas)
        if not node_in: condition = ~condition
        conditions.append(condition)
    if period: conditions.append(df['Periode'] == period)
    flows = df[np.bitwise_and.reduce(conditions)]

    # groupby columns
    groupby = [
        'source',
        'target',
        'Gewicht_KG'
    ]

    columns_within = {
        f'{source}_{level}': 'source',
        f'{target}_{level}': 'target',
    }

    # spli to flows within Netherlands & abroad
    conditions = []
    for node, node_in in zip([source, target], [source_in, target_in]):
        if not node_in:
            condition = (flows[f'{node}_Land'] == 'NEDERLAND') |\
                        (flows[f'{node}_Land'] == 'NAN')
            conditions.append(condition)
    flows_within = flows[np.bitwise_and.reduce(conditions)]
    flows_abroad = flows[~np.bitwise_and.reduce(conditions)]

    # flows = flows within Netherlands + flows abroad
    assert len(flows) == len(flows_abroad) + len(flows_within)

    # preprocess
    flows_within = flows_within.rename(columns=columns_within)
    source_level = f'{source}_{level}' if source_in else f'{source}_Continent'
    target_level = f'{target}_{level}' if target_in else f'{target}_Continent'
    columns_abroad = {
        source_level: 'source',
        target_level: 'target'
    }
    flows_abroad = flows_abroad.rename(columns=columns_abroad)

    flows_within = flows_within[groupby]
    flows_abroad = flows_abroad[groupby]
    flows = pd.concat([flows_within, flows_abroad])

    # aggregate
    groups = flows.groupby(groupby[:-1]).sum().reset_index()

    return to_flowmap(groups, level=level)


def get_activities(df, period=None, source=None,
                   level=None, areas=None):
    activities = ACTIVITIES['code'].to_list()
    processes = PROCESSES['code'].to_list()
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
            'activity': activities,
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
    flows['VerwerkingsGroep'] = flows['VerwerkingsmethodeCode'].str[0]
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
            'activity': activities,
            'process': processes,
            'values': {
                'weight': {
                    'value': weight,
                    'unit': 'ktn'
                }
            }
        })

    return results


def get_network(df,
                period=None,
                source=None, source_in=True,
                target=None, target_in=True,
                level=None, areas=[],
                routings=None):
    # filter dataframe
    conditions = []
    for node, node_in in zip([source, target], [source_in, target_in]):
        condition = df[f'{node}_{level}'].isin(areas)
        if not node_in: condition = ~condition
        conditions.append(condition)
    if period: conditions.append(df['Periode'] == period)
    flows = df[np.bitwise_and.reduce(conditions)]

    # join with routings
    flows = pd.merge(flows, routings, how='left', on=['origin', 'destination'])
    unmatched = len(flows[flows['origin'].isnull()])
    if unmatched:
        print(f'No routings for {unmatched.index} flows...')

    # TODO: fill with random numbers (Aantal_vrachten)
    vehicles = pd.read_excel('data/network/vehicle.xlsx')
    flows['Aantal_vrachten'] = np.random.randint(1, 10, flows.shape[0])
    flows['tn'] = flows['Gewicht_KG'] / 10**3
    flows['average'] = flows['tn'] / flows['Aantal_vrachten']

    # TODO: compute CO2 emissions
    for idx, row in vehicles.iterrows():
        min, max = row['min'], row['max']
        condition = (flows['average'] >= min) & (flows['average'] < max)
        flows.loc[condition, 'grams per tonne kilometer'] = row['co2']
    flows['co2'] = flows['grams per tonne kilometer'] * flows['tn'] * flows['distance'] / 10**3

    # totals
    area = f'{source}_{level}' if source_in else f'{target}_{level}'
    groupby = [
        area,
        'co2'
    ]
    groups = flows.groupby(groupby[:-1]).sum().reset_index()

    # network map for province
    # if level == 'Provincie':
        # # distribute along network
        # ways = {}
        # for idx, flow in flows.iterrows():
        #     seq, amount = flow['seq'], flow['co2']
        #     if not amount: continue
        #
        #     if type(seq) == str:
        #         seq = [id for id in seq.split('@')]
        #         for id in seq:
        #             if id in ways:
        #                 ways[id] += amount
        #             else:
        #                 ways[id] = amount
        #
        # data = []
        # for way in NETWORK.items():
        #     id, geometry = way
        #     id = str(id)
        #     if id not in ways: ways[id] = 0
        #     data.append({
        #         'geometry': geometry,
        #         'amount': ways[id]
        #     })
        #
        # return [{
        #     'id': key,
        #     'value': value
        # } for key, value in ways.items()]


if __name__ == "__main__":
    # import areas
    print('Import areas...')
    gemeenten = gpd.read_file('data/areas/gemeenten.shp')
    gemeenten['centroid'] = gemeenten['geometry'].centroid
    AREAS['Gemeente'] = gemeenten

    provincies = gpd.read_file('data/areas/provincies.shp')
    provincies['centroid'] = provincies['geometry'].centroid
    AREAS['Provincie'] = provincies

    continents = gpd.read_file('data/areas/continents.shp')
    continents['centroid'] = continents['geometry'].centroid
    AREAS['Continent'] = continents

    countries = gpd.read_file('data/areas/countries.shp')
    countries['country_nl'] = countries['country_nl'].str.upper()
    countries = pd.merge(countries, continents[['cont_en', 'cont_nl']], how='left', on='cont_en')
    AREAS['Country'] = countries

    provincie = var.PROVINCE
    provincie_gemeenten = gemeenten[gemeenten['parent'] == provincie]['name'].to_list()

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

            # group flows to periods
            print('Split to periods...')
            df = group_flows(df, period=period)

            # add areas to roles
            print('Add areas to roles...')
            source = ROLES[typ]['source']  # source role
            target = ROLES[typ]['target']  # target role
            activity = ROLES[typ]['activity']  # activity role (ontvangst: 'ontdoener')
            for role, level in itertools.product([source, target], [
                'Provincie',
                # 'Gemeente'
            ]):
                areas = AREAS[level]
                df = add_areas(df, areas=areas, role=role, admin_level=level)
            if typ == 'Ontvangst':
                df = add_areas(df, areas=AREAS['Provincie'], role=activity, admin_level="Provincie")
                # df = add_areas(df, areas=AREAS['Gemeente'], role=activity, admin_level="Gemeente")

            # add continents based on countries to roles
            countries = countries[['country_nl', 'cont_nl']]
            for role in [source, target]:
                columns = list(df.columns)
                df = pd.merge(df, countries, how='left', left_on=f'{role}_Land', right_on='country_nl')
                df[f'{role}_Continent'] = df['cont_nl']
                columns.append(f'{role}_Continent')
                df = df[columns]

            # add identifiers
            print('Add identifiers to flows...')
            df = add_identifiers(df, type=typ)

            # analyse on provincial & municipal level
            print('Analyse...')
            for level in [
                'Provincie',
                # 'Gemeente'
            ]:
                areas = [provincie] if level == 'Provincie' else provincie_gemeenten

                prefixes = {
                    'Provincie': 'province',
                    'Gemeente': 'municipality',
                    'Ontvangst': 'primary',
                    'Afgifte': 'secondary',
                }
                prefix = f'{prefixes[level]}\t{prefixes[typ]}_waste'

                for p in periods:
                    suffix = f'{year}'
                    if p: suffix = f'{suffix}-{period[0].upper()}{p}'

                    # import (source out, target in) -> FLOWMAP
                    RESOURCE[f'{prefix}_import\t{suffix}'] = \
                        get_flows(df,
                                  period=p,
                                  source=source, source_in=False,
                                  target=target, target_in=True,
                                  level=level, areas=areas)

                    # # internal (source in, target in) -> FLOWMAP
                    # RESOURCE[f'{prefix}_internal\t{suffix}'] = \
                    #     get_flows(df,
                    #               period=p,
                    #               source=source, source_in=True,
                    #               target=target, target_in=True,
                    #               level=level, areas=areas)
                    #
                    # # export (source in, target out) -> FLOWMAP
                    # RESOURCE[f'{prefix}_export\t{suffix}'] = \
                    #     get_flows(df,
                    #               period=p,
                    #               source=source, source_in=True,
                    #               target=target, target_in=False,
                    #               level=level, areas=areas)

                    # # economic activities -> GRAPHS
                    # DATA[f'{prefix}_activities\t{suffix}'] = \
                    #     get_activities(df,
                    #                    period=p,
                    #                    source=activity,
                    #                    level=level, areas=areas)
                    #
                    # # economic activities -> FLOWMAPS
                    # RESOURCE[f'{prefix}_activities\t{suffix}'] = \
                    #     get_flows(df,
                    #               period=p,
                    #               source=source,
                    #               target=target,
                    #               level=level, areas=areas)

                    # # transport
                    # DATA[f'{prefix}_import_co2\t{suffix}'] = \
                    #     get_network(df,
                    #                 period=p,
                    #                 source=activity, source_in=False,
                    #                 target=target, target_in=True,
                    #                 level=level, areas=areas,
                    #                 routings=routings)

                    # DATA[f'{prefix}_export_co2_{suffix}'] = \
                    #     get_network(df,
                    #                 period=p,
                    #                 source=activity, source_in=True,
                    #                 target=target, target_in=False,
                    #                 level=level, areas=areas,
                    #                 routings=routings)

        print('\n')

    # results = {}
    # with open('test/overview.json', 'w') as outfile:
    #     for key, items in DATA.items():
    #         level, field, period = key.split('\t')
    #         for item in items:
    #             item['level'] = level
    #             item['period'] = period
    #             results.setdefault(field, []).append(item)
    #     json.encoder._make_iterencode = _make_iterencode._make_iterencode
    #     indent = (2, None)
    #     json.dump(results, outfile, indent=indent)

    with open('test/overview_resource.json', 'w') as outfile:
        results = []
        for key, items in RESOURCE.items():
            level, field, period = key.split('\t')
            type = field.replace('_', ' ')
            for item in items:
                item['type'] = type
                results.append(item)
        json.dump(results, outfile, indent=4)



