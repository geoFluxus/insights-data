import pandas as pd
import geopandas as gpd
import json
import variables as var
import numpy as np


INPUTS = [
    {
        'year': 2020,
        'period': 'year', # year
    },
]

NETWORK = {}

MAP = {} # map data


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


if __name__ == "__main__":
    # import routings
    # print('Import routings...')
    routings = pd.read_csv(f'../../../../../media/geofluxus/DATA/national/routings.csv', low_memory=False, sep=';')
    routings['distance'] = gpd.GeoSeries.from_wkt(routings[routings['wkt'].notnull()]['wkt'])\
                                        .set_crs('epsg:4326')\
                                        .to_crs('epsg:3857')\
                                        .length
    # routings.to_csv('data/network/routings.csv', index=False, sep=';')

    # import network
    with open('../data/network/network.geojson') as f:
        geojson = json.load(f)
        for feat in geojson['features']:
            id = str(feat['properties']['id'])
            NETWORK[id] = feat['geometry']

    prefixes = {
        'Provincie': 'province',
        'Gemeente': 'municipality',
        'Ontvangst': 'primary',
        'Afgifte': 'secondary',
    }

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

            # add identifiers
            print('Add identifiers to flows...')
            df = add_identifiers(df, type=typ)

            # add routings
            print('Add routings to flows...')
            df = add_routings(df)

            # CO2 NETWORK MAP (all levels)
            MAP.setdefault('transport', {})[f'{prefixes[typ]}_waste\tco2'] = get_network(df)

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
            'amount': round(ways[id] / 10**6, 2),  # grams -> tn
            'period': f'{year}'
        })
    with open('../test/co2_network.json', 'w') as outfile:
        json.dump(results, outfile, indent=4)