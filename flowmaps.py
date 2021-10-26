import pandas as pd
import geopandas as gpd
import variables as var
import itertools
import json
import numpy as np


INPUTS = [
    {
        'year': 2021,
        'period': 'quarter',  # quarter
        'periods': [2]
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

AREAS = {}

ACTIVITIES = None

MAP = {} # map data


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


if __name__ == "__main__":
    # import areas
    print('Import areas...')
    provincie_gemeenten = import_areas()
    provincie = var.PROVINCE

    # import activities
    ACTIVITIES = pd.read_excel('data/flows/activitygroup.xlsx')
    ACTIVITIES['name'] = ACTIVITIES['name'].str.lower().str.capitalize()

    # import processes
    TREATMENT_METHODS = {
        'landfill': ['G01', 'G02'],
        'incineration': ['B04', 'F01', 'F02', 'F06', 'F07'],
        'reuse': ['B01', 'B03', 'B05'],
        'recycling': ['C01', 'C02', 'C03', 'C04', 'D01',
                      'D02', 'D03', 'D04', 'D05', 'D06',
                      'E01', 'E02', 'E03', 'E04', 'E05',
                      'F03', 'F04'],
        'storage': ['A01', 'A02']
    }

    for input in INPUTS:
        year = input['year']
        period = input['period']
        periods = input.get('periods', [None])

        print(f'YEAR: {year}')

        # iterate all flow types
        for typ in [
            'Ontvangst'
        ]:
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
            for role, level in itertools.product([source, target], ['Provincie', 'Gemeente']):
                areas = AREAS[level]
                df = add_areas(df, areas=areas, role=role, admin_level=level)

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
            for name, codes in TREATMENT_METHODS.items():
                df.loc[df['VerwerkingsmethodeCode'].isin(codes), 'Process'] = name

            prefixes = {
                'Provincie': 'province',
                'Gemeente': 'municipality',
                'Ontvangst': 'primary',
                'Afgifte': 'secondary',
            }

            # analyse on provincial & municipal level
            print('Analyse...')
            for level in [
                'Provincie',
                'Gemeente'
            ]:
                areas = [provincie] if level == 'Provincie' else provincie_gemeenten

                prefix = f'{prefixes[level]}\t{prefixes[typ]}_waste'

                for p in periods:
                    suffix = f'{year}'
                    if p: suffix = f'{suffix}-{period[0].upper()}{p}'

                    if prefixes[typ] == 'primary':
                        # FLOWMAPS
                        if prefixes[level] == 'municipality':
                            # economic activities (Herkomst in) -> FLOWMAP
                            MAP.setdefault('economic_sectors', {})[f'{prefix}_activity\t{suffix}'] = \
                                get_flows(df,
                                          period=p,
                                          source=source, source_in=True,
                                          target=target,
                                          level=level, areas=areas,
                                          groupby=['Activity'],
                                          rename={'Activity': 'activity'})

                            # waste processes (Herkomst in) -> FLOWMAP
                            MAP.setdefault('treatment_methods', {})[f'{prefix}_process\t{suffix}'] = \
                                get_flows(df,
                                          period=p,
                                          source=source, source_in=True,
                                          target=target,
                                          level=level, areas=areas,
                                          groupby=['Process'],
                                          rename={'Process': 'process'})

    # FLOWMAPS
    for section, data in MAP.items():
        with open(f'test/{section}_flowmap.json', 'w') as outfile:
            results = []
            for key, items in data.items():
                level, field, period = key.split('\t')
                type = field.replace('_', ' ')
                for item in items:
                    item['period'] = period
                    item['type'] = type
                    results.append(item)
            json.dump(results, outfile, indent=4)