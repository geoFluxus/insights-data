import pandas as pd
import geopandas as gpd
import json
import variables as var
import numpy as np
import utils


VARS = {
    'INPUT_DIR': var.INPUT_DIR,
    'AREA': var.AREA,
    'LEVEL': var.LEVEL,
    'YEAR': var.YEAR,
    'OUTPUT_DIR': var.OUTPUT_DIR
}

NETWORK = {}

MAP = {} # map data

DATA = {}


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
    flows = pd.merge(flows, routings, how='left', on=['origin', 'destination'], indicator=True)
    unmatched = flows[flows._merge=='left_only']
    if len(unmatched):
        print(f'No routings for {len(unmatched)} flows...')

    # average payload per trip
    vehicles = pd.read_excel(f"{VARS['INPUT_DIR']}/GEODATA/network/vehicle.xlsx")
    flows['tn'] = flows['Gewicht_KG'] / 10**3
    flows['average'] = flows['tn'] / flows['Aantal_vrachten']

    # compute co2 emissions
    for idx, row in vehicles.iterrows():
        min, max = row['min'], row['max']
        condition = (flows['average'] >= min) & (flows['average'] < max)
        flows.loc[condition, 'grams per tonne kilometer'] = row['co2']
    flows['co2'] = flows['grams per tonne kilometer'] * flows['tn'] * flows['distance'] / 10**3

    return flows


def get_network(df):
    # distribute along network
    ways = {}
    for idx, flow in df.iterrows():
        seq, amount = flow['seq'], flow['co2'] / (flow['distance'] / 10**3)
        if type(seq) == str:
            seq = [id for id in seq.split('@')]
            if np.isnan(amount): amount = 0
            for id in seq:
                if id in ways:
                    ways[id] += amount
                else:
                    ways[id] = amount

    return ways


def get_emissions(df,
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
                    'unit': 'kt'
                }
            }
        })

    return results


if __name__ == "__main__":
    PREFIXES = var.PREFIXES
    ROLES = var.ROLES

    # start analysis
    print('NETWORKMAP ANALYSIS')
    print('VARIABLES:')
    for name, value in VARS.items():
        print(f'{name}={value}')

    # import routings
    print('Import routings...')
    routings = pd.read_csv(f"./json/routings.csv", low_memory=False, sep=';')
    # routings = pd.read_csv(f"{VARS['INPUT_DIR']}/GEODATA/network/routings.csv", low_memory=False, sep=';')
    routings.loc[routings['distance'] == 'None', 'distance'] = np.nan
    routings['distance'] = routings['distance'].astype('float')

    # import network
    with open(f"{VARS['INPUT_DIR']}/GEODATA/network/network.geojson") as f:
        geojson = json.load(f)
        for feat in geojson['features']:
            id = str(feat['properties']['id'])
            distance = feat['properties']['distance']
            NETWORK[id] = (feat['geometry'], distance)

    # import areas
    # import province polygon
    polygon = utils.import_areas(level=VARS['LEVEL'])
    polygon = polygon[polygon['name'] == VARS['AREA']]
    assert len(polygon) == 1

    # start analysis
    print(f"YEAR: {VARS['YEAR']}")

    # iterate all flow types
    for typ in ['Ontvangst', 'Afgifte']:
        # import file
        print()
        print(f'Import {typ}....')
        path = f"{VARS['INPUT_DIR']}/{VARS['LEVEL']}{VARS['AREA']}/LMA/processed"
        filename = f"{path}/{typ.lower()}_{VARS['AREA'].lower()}_{VARS['YEAR']}_full.csv"
        df = pd.read_csv(filename, low_memory=False)

        # add identifiers
        print('Add identifiers to flows...')
        df = add_identifiers(df, type=typ)

        # add routings
        print('Add routings to flows...')
        df = add_routings(df)

        # add areas to roles
        print('Add areas to roles...')
        source = ROLES[typ]['source']  # source role
        target = ROLES[typ]['target']  # target role
        for role in [source, target]:
            df = utils.add_areas(df,
                                 areas=polygon,
                                 role=role,
                                 admin_level=VARS['LEVEL'])

        # compute emissions
        # import emissions (source out, target in)
        DATA[f"{PREFIXES[typ]}_import_co2\t{VARS['YEAR']}"] = \
            get_emissions(df,
                          source=source, source_in=False,
                          target=target, target_in=True,
                          level=VARS['LEVEL'], areas=[VARS['AREA']])

        # export emissions (source in, target out)
        DATA[f"{PREFIXES[typ]}_export_co2\t{VARS['YEAR']}"] = \
            get_emissions(df,
                          source=source, source_in=True,
                          target=target, target_in=False,
                          level=VARS['LEVEL'], areas=[VARS['AREA']])

        # internal emissions (source in, target in)
        DATA[f"{PREFIXES[typ]}_internal_co2\t{VARS['YEAR']}"] = \
            get_emissions(df,
                          source=source, source_in=True,
                          target=target, target_in=True,
                          level=VARS['LEVEL'], areas=[VARS['AREA']])

        # CO2 NETWORK MAP (all levels)
        MAP.setdefault('transport', {})[f'{PREFIXES[typ]}_waste\tco2'] = get_network(df)

    print(json.dumps(DATA, indent=4))

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
        id, props = way
        geometry, distance = props
        id = str(id)
        if id not in ways: ways[id] = 0
        results.append({
            'id': id,
            'geometry': geometry,
            'amount': round(ways[id] * (distance / 10**3) / 10**6, 2),  # grams -> tn
            'period': f"{VARS['YEAR']}"
        })
    with open(f"{VARS['OUTPUT_DIR']}/overview_co2_network.json", 'w') as outfile:
        json.dump(results, outfile, indent=4)