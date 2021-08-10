import pandas as pd
import geopandas as gpd
import variables as var
import itertools
import numpy as np


DATA = {}


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
    columns = {
        f'{source}_Land': 'source',
        f'{target}_Land': 'target'
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

    return groups


if __name__ == "__main__":
    # import areas
    gemeenten = gpd.read_file('data/areas/gemeenten.shp')
    provincies = gpd.read_file('data/areas/provincies.shp')
    levels = {
        'Gemeente': gemeenten,
        'Provincie': provincies
    }
    provincie = var.PROVINCE
    provincie_gemeenten = gemeenten[gemeenten['parent'] == provincie]['name'].to_list()

    INPUTS = [
        {
            'year': 2020,
            'period': 'year', # year
        },
        {
            'year': 2021,
            'period': 'quarter', # quarter
            'periods': [1, 2]
        }
    ]

    ROLES = {
        'Ontvangst': {
            'source': 'Herkomst',
            'target': 'Verwerker'
        },
        'Afgifte': {
            'source': 'EerstAfnemer',
            'target': 'Verwerker'
        }
    }

    for input in INPUTS:
        year = input['year']
        period = input['period']
        periods = input.get('periods', [None])

        print(f'YEAR: {year}')

        for type in ['Ontvangst', 'Afgifte']:
            # import file
            print('')
            print(f'Import {type}....')
            path = f'data/flows/{type.lower()}_utrecht_{year}.csv'
            df = pd.read_csv(path, low_memory=False)

            # group flows to periods
            print('Split to periods...')
            df = group_flows(df, period=period)

            # add areas to roles
            print('Add areas to roles...')
            source = ROLES[type]['source']
            target = ROLES[type]['target']
            for role, level in itertools.product([source, target], levels.keys()):
                areas = levels[level]
                df = add_areas(df, areas=areas, role=role, admin_level=level)

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
                prefix = f'{prefixes[level]}_{prefixes[type]}_waste'

                for p in periods:
                    suffix = f'{year}'
                    if p: suffix = f'{suffix}_{period[0]}{p}'

                    # import (source in, target out)
                    DATA[f'{prefix}_import_{suffix}'] = \
                        get_flows(df,
                                  period=p,
                                  source=source, source_in=True,
                                  target=target, target_in=False,
                                  level=level, areas=areas)

                    # # export (source out, target in)
                    DATA[f'{prefix}_export_{suffix}'] = \
                        get_flows(df,
                              period=p,
                              source=source, source_in=False,
                              target=target, target_in=True,
                              level=level, areas=areas)

        print('\n')

    print(DATA.keys())

