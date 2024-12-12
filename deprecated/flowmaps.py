import pandas as pd
import geopandas as gpd
import variables as var
import itertools
import json
import numpy as np
from src.analysis import utils

VARS = {
    'INPUT_DIR': var.INPUT_DIR,
    'AREA_DIR': var.AREA_DIR,
    'AREA': var.AREA,
    'LEVEL': var.LEVEL,
    'POSTCODES': var.POSTCODES,
    'YEAR': var.YEAR,
    'OUTPUT_DIR': var.OUTPUT_DIR,
}

AREAS = {}

ACTIVITIES = None

MAP = {} # map data


def import_areas():
    # municipalities
    gemeenten = gpd.read_file(f"{VARS['INPUT_DIR']}/GEODATA/areas/gemeenten/gemeenten_{var.YEAR}.shp")
    gemeenten['centroid'] = gemeenten['geometry'].centroid
    # gemeenten['centroid'] = gemeenten['geometry'].to_crs(epsg=3857).centroid.to_crs(epsg=4326)
    AREAS['Gemeente'] = gemeenten

    # # continents
    # continents = gpd.read_file('./data/areas/continents.shp')
    # continents['centroid'] = continents['geometry'].centroid
    # # continents['centroid'] = continents['geometry'].to_crs(epsg=3857).centroid.to_crs(epsg=4326)
    # AREAS['Continent'] = continents

    # countries
    # countries = gpd.read_file(f"{VARS['INPUT_DIR']}/GEODATA/areas/countries/countries.shp")
    # countries['country_nl'] = countries['country_nl'].str.upper()
    # countries = pd.merge(countries, continents[['cont_en', 'cont_nl']], how='left', on='cont_en')
    # AREAS['Country'] = countries

    # import postcodes
    postcodes = pd.read_csv(f"{VARS['INPUT_DIR']}/GEODATA/postcodes/{VARS['POSTCODES']}.csv", low_memory=False)
    postcodes['PC4'] = postcodes['PC4'].astype(str)
    gemeenten = postcodes[['Gemeente', 'Provincie']].drop_duplicates()
    area_gemeenten = gemeenten[gemeenten[f"{VARS['LEVEL']}"] == VARS['AREA']]['Gemeente'].to_list()

    # list of area municipalities
    gemeenten = AREAS['Gemeente']
    area_gemeenten = gemeenten[gemeenten['name'].isin(area_gemeenten)]['name'].to_list()

    return area_gemeenten


def to_flowmap(df, level=None, extra=[]):
    level_areas = AREAS[level]
    # continents = AREAS['Continent']

    # for field, areas in zip(['name', 'cont_nl'], [level_areas, continents]):
    for field, areas in zip(['name'], [level_areas]):
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

    # # split to flows with source/target in/out Netherlands
    # for node in [source, target]:
    #     condition = (flows[f'{node}_Land'] == 'NEDERLAND') |\
    #                 (flows[f'{node}_Land'] == 'NAN')
    #     flows.loc[condition, f'{node}_in'] = True
    #     flows.loc[~condition, f'{node}_in'] = False
    #     flows.loc[flows[f'{node}_in'] == True, columns[f'{node}_{level}']] = flows[f'{node}_{level}']
    #     flows.loc[flows[f'{node}_in'] == False, columns[f'{node}_{level}']] = flows[f'{node}_Continent']

    # aggregate
    for node in [source, target]:
        flows[columns[f'{node}_{level}']] = flows[f'{node}_{level}']
    groups = flows[_groupby].groupby(_groupby[:-1]).sum().reset_index()
    groups = groups.rename(columns=rename)

    return to_flowmap(groups, level=level, extra=extra)


if __name__ == "__main__":
    ROLES = var.ROLES
    PREFIXES = var.PREFIXES
    TREATMENT_METHODS = var.TREATMENT_METHODS

    # start analysis
    print('FLOWMAP ANALYSIS')
    print('VARIABLES:')
    for name, value in VARS.items():
        print(f'{name}={value}')

    # import areas
    print()
    print('Import areas...')
    area_gemeenten = import_areas()
    print(f'AREA GEMEENTEN ({len(area_gemeenten)}): {area_gemeenten}')

    # import activities
    ACTIVITIES = pd.read_excel(f"{VARS['INPUT_DIR']}/DATA/descriptions/activitygroup.xlsx")
    ACTIVITIES['name'] = ACTIVITIES['code'] + ' - ' + ACTIVITIES['name_nl'].str.lower().str.capitalize()

    # start analysis
    print(f"YEAR: {VARS['YEAR']}")

    # LMA Ontvangst
    for typ in ['Ontvangst']:
        # import file
        print()
        print(f'Import {typ}....')
        path = f"{VARS['INPUT_DIR']}/{VARS['AREA_DIR']}/LMA/processed"
        filename = f"{path}/ontvangst_{VARS['AREA'].lower()}_{VARS['YEAR']}_full.csv"
        df = pd.read_csv(filename, low_memory=False)

        # add areas to roles
        print('Add areas to roles...')
        source = ROLES[typ]['source']  # source role
        target = ROLES[typ]['target']  # target role
        activity = ROLES[typ]['activity']  # activity role (ontvangst: 'ontdoener')
        for role, level in itertools.product([source, target], ['Gemeente']):
            areas = AREAS[level]
            df = utils.add_areas(df, areas=areas, role=role, admin_level=level)

        # # add continents based on countries to roles
        # countries = AREAS['Country']
        # countries = countries[['country_nl', 'cont_nl']]
        # for role in [source, target]:
        #     columns = list(df.columns)
        #     df = pd.merge(df, countries, how='left', left_on=f'{role}_Land', right_on='country_nl')
        #     df[f'{role}_Continent'] = df['cont_nl']
        #     columns.append(f'{role}_Continent')
        #     df = df[columns]

        # add activity names
        columns = list(df.columns)
        df = pd.merge(df, ACTIVITIES, how='left', left_on=f'{activity}_AG', right_on='code')
        df = df.rename(columns={'name': 'Activity'})
        columns.append('Activity')
        df = df[columns]

        # add process names
        for name, codes in TREATMENT_METHODS.items():
            df.loc[df['VerwerkingsmethodeCode'].isin(codes), 'Process'] = name.capitalize()

        # analyse on municipal level
        print('Analyse...')
        for level in ['Gemeente']:
            areas = [VARS['AREA']] if level == 'Provincie' else area_gemeenten

            # data prefix
            prefix = f'{PREFIXES[level]}\t{PREFIXES[typ]}_waste'

            # economic activities (Herkomst in)
            MAP.setdefault('overview_activities', {})[f"{prefix}_activity\t{VARS['YEAR']}"] = \
                get_flows(df,
                          source=source, source_in=True,
                          target=target,
                          level=level, areas=areas,
                          groupby=['Activity'],
                          rename={'Activity': 'activity'})

            # waste processes (Herkomst in)
            MAP.setdefault('overview_processes', {})[f"{prefix}_process\t{VARS['YEAR']}"] = \
                get_flows(df,
                          source=source, source_in=True,
                          target=target,
                          level=level, areas=areas,
                          groupby=['Process'],
                          rename={'Process': 'process'})

    # FLOWMAPS
    for section, data in MAP.items():
        with open(f"{VARS['OUTPUT_DIR']}/{section}_flowmap.json", 'w') as outfile:
            results = []
            for key, items in data.items():
                level, field, period = key.split('\t')
                type = field.replace('_', ' ')
                for item in items:
                    item['period'] = period
                    item['type'] = type
                    results.append(item)
            json.dump(results, outfile, indent=4)