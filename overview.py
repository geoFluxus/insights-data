import pandas as pd
import geopandas as gpd
import variables as var
import itertools
import numpy as np
import json


DATA = {}

AREAS = {}


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


def to_flowmap(df, level=None):
    level_areas = AREAS[level]
    countries = AREAS['Country']

    for field, areas in zip(['name', 'country_nl'], [level_areas, countries]):
        for node in ['source', 'target']:
            columns = list(df.columns)
            df = pd.merge(df, areas, how='left', left_on=node, right_on=field)
            if f'{node}_centroid' not in columns:
                df[f'{node}_centroid'] = df['centroid']
                columns.append(f'{node}_centroid')
            else:
                df.loc[df[f'{node}_centroid'].isnull(), f'{node}'] = df['country_en']
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

    # flows within Netherlands
    conditions = []
    for node, node_in in zip([source, target], [source_in, target_in]):
        if not node_in:
            condition = (flows[f'{node}_Land'] == 'NEDERLAND') |\
                        (flows[f'{node}_Land'] == 'NAN')
            conditions.append(condition)
    flows_within = flows[np.bitwise_and.reduce(conditions)]

    # flows abroad
    conditions = []
    for node, node_in in zip([source, target], [source_in, target_in]):
        if not node_in:
            condition = (flows[f'{node}_Land'] != 'NEDERLAND') &\
                        (flows[f'{node}_Land'] != 'NAN')
            conditions.append(condition)
    flows_abroad = flows[np.bitwise_and.reduce(conditions)]

    # flows = flows within Netherlands + flows abroad
    assert len(flows) == len(flows_abroad) + len(flows_within)

    # preprocess
    columns = {
        f'{source}_{level}': 'source',
        f'{target}_{level}': 'target',
    }
    flows_within = flows_within.rename(columns=columns)
    source_level = f'{source}_{level}' if source_in else f'{source}_Land'
    target_level = f'{target}_{level}' if target_in else f'{target}_Land'
    columns = {
        source_level: 'source',
        target_level: 'target'
    }
    flows_abroad = flows_abroad.rename(columns=columns)

    # join
    groupby = [
        'source',
        'target',
        'Gewicht_KG'
    ]
    flows_within = flows_within[groupby]
    flows_abroad = flows_abroad[groupby]
    flows = pd.concat([flows_within, flows_abroad])

    # aggregate
    groups = flows.groupby(groupby[:-1]).sum().reset_index()

    return to_flowmap(groups, level=level)


def get_activities(df, period=None, source=None,
                   level=None, areas=None):
    # filter source in areas
    flows = df[df[f'{source}_{level}'].isin(areas)]
    flows = flows.rename(columns={source: 'source'})

    # filter by period
    if period: flows = flows[flows['Periode'] == period]

    # group by economic activity
    groups = flows.groupby(f'{source}_NACE')\
                  .agg({'Gewicht_KG': ['count', 'sum']})\
                  .reset_index()
    groups.columns = ['_'.join(col).strip() for col in groups.columns.values]
    groups = groups.rename(columns={
        f'{source}_NACE_': 'activity',
        'Gewicht_KG_count': 'count',
        'Gewicht_KG_sum': 'sum'
    })

    # TODO: Add activity descriptions in Dutch
    results = []
    for idx, row in groups.iterrows():
        results.append({
            'activity': row['activity'],
            'count': row['count'],
            'total': row['sum']
        })

    return results


if __name__ == "__main__":
    # import areas
    gemeenten = gpd.read_file('data/areas/gemeenten.shp')
    gemeenten['centroid'] = gemeenten['geometry'].centroid
    AREAS['Gemeente'] = gemeenten

    provincies = gpd.read_file('data/areas/provincies.shp')
    provincies['centroid'] = provincies['geometry'].centroid
    AREAS['Provincie'] = provincies

    countries = gpd.read_file('data/areas/countries.shp')
    countries['country_nl'] = countries['country_nl'].str.upper()
    countries['centroid'] = countries['geometry'].centroid
    AREAS['Country'] = countries

    provincie = var.PROVINCE
    provincie_gemeenten = gemeenten[gemeenten['parent'] == provincie]['name'].to_list()

    INPUTS = [
        # {
        #     'year': 2020,
        #     'period': 'year', # year
        # },
        {
            'year': 2021,
            'period': 'quarter', # quarter
            'periods': [1, 2]
        }
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

    for input in INPUTS:
        year = input['year']
        period = input['period']
        periods = input.get('periods', [None])

        print(f'YEAR: {year}')

        for typ in ['Ontvangst', 'Afgifte']:
            # import file
            print('')
            print(f'Import {typ}....')
            path = f'data/flows/{typ.lower()}_utrecht_{year}.csv'
            df = pd.read_csv(path, low_memory=False)

            # group flows to periods
            print('Split to periods...')
            df = group_flows(df, period=period)

            # add areas to roles
            print('Add areas to roles...')
            source = ROLES[typ]['source']
            target = ROLES[typ]['target']
            activity = ROLES[typ]['activity']
            for role, level in itertools.product([source, target], ['Provincie', 'Gemeente']):
                areas = AREAS[level]
                df = add_areas(df, areas=areas, role=role, admin_level=level)
            if typ == 'Ontvangst':
                df = add_areas(df, areas=AREAS['Provincie'], role=activity, admin_level="Provincie")
                df = add_areas(df, areas=AREAS['Gemeente'], role=activity, admin_level="Gemeente")

            # analyse on provincial & municipal level
            print('Analyse...')
            for level in ['Provincie', 'Gemeente']:
                areas = [provincie] if level == 'Provincie' else provincie_gemeenten

                prefixes = {
                    'Provincie': 'prov',
                    'Gemeente': 'muni',
                    'Ontvangst': 'primary',
                    'Afgifte': 'secondary',
                }
                prefix = f'{prefixes[level]}_{prefixes[typ]}_waste'

                for p in periods:
                    suffix = f'{year}'
                    if p: suffix = f'{suffix}_{period[0]}{p}'

                    # # import (source out, target in)
                    # DATA[f'{prefix}_import_{suffix}'] = \
                    #     get_flows(df,
                    #               period=p,
                    #               source=source, source_in=False,
                    #               target=target, target_in=True,
                    #               level=level, areas=areas)
                    #
                    # # export (source in, target out)
                    # DATA[f'{prefix}_export_{suffix}'] = \
                    #     get_flows(df,
                    #               period=p,
                    #               source=source, source_in=True,
                    #               target=target, target_in=False,
                    #               level=level, areas=areas)

                    # economic activities
                    DATA[f'{prefix}_activities_{suffix}'] = \
                        get_activities(df,
                                       period=p,
                                       source=activity,
                                       level=level, areas=areas)

        print('\n')

    with open('test/overview.json', 'w') as outfile:
        json.dump(DATA, outfile, indent=4)

