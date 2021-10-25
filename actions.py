import variables as var
import pandas as pd
import geopandas as gpd
from sklearn.linear_model import LinearRegression
import numpy as np
import logging
import itertools
import json


DATA = {}

RESULTS = {}


def to_json(value):
    if np.isnan(value): return 0
    return round(value, 2)


def get_flows(year=None):
    """
    Import flows from 'data/flows'
    """
    path = f'../../../../../media/geofluxus/DATA/national/{var.PROVINCE.lower()}/processed'
    filename = f'{path}/ontvangst_{var.PROVINCE.lower()}_{year}.csv'

    return pd.read_csv(filename, low_memory=False)


def get_areas():
    """
    Import areas from 'data/areas'
    """
    provincies = gpd.read_file('./data/areas/provincies.shp')
    gemeenten = gpd.read_file('./data/areas/gemeenten.shp')
    return provincies, gemeenten


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


def save(flows, area=None):
    X, Y = [], []
    level, field, category, typ, unit = [None] * 5
    for key, amount in flows.items():
        level, field, category, typ, period, unit = key.split('\t')
        X.append(period)
        Y.append(to_json(amount))
    RESULTS.setdefault(field, []).append({
        'name': area,
        'level': level,
        'category': category,
        'type': typ,
        'period': X,
        'values': {
            'waste': {
                'weight': {
                    'value': Y,
                    'unit': unit
                }
            }
        }
    })


def compute_trends(df, on=[], values=[], per_months=12, prop=None, add_graph=True, add_trends=True):
    """
    Analyse trends for areas on different timeframe
    df: flow dataframe -> DataFrame
    on: properties to groupby -> list
    values: contains list of values for each property to select -> list
    per_months: timeframe to analyse (year=12, quarter=3 etc.) -> int
    """
    # columns to select & groupby
    columns = on.copy()
    columns.extend([
        'MeldPeriodeJAAR',
        'Gewicht_TN'
    ])
    groupby = columns[:-1]

    # split months into periods
    months = list(range(1, 13))
    periods = [
        months[i: i + per_months] for i in range(0, len(months), per_months)
    ]

    # aggregate into periods of months for each year
    df_new = []
    for year in var.YEARS:
        # aggregate per period & store to new df
        for idx, period in enumerate(periods):
            df_q = df[
                (df['MeldPeriodeJAAR'] == year) &
                (df['MeldPeriodeMAAND'].isin(period))
                ]
            df_q = df_q[columns].groupby(groupby).sum().reset_index()
            df_q['Periode'] = idx + 1
            df_new.append(df_q)

    # concatenate for all periods & years
    df_new = pd.concat(df_new)

    # find unique values for properties if not provided
    new_values = []
    for idx, value in enumerate(values):
        if not value:
            value = df_new.drop_duplicates(on[idx])
            value = value[on[idx]].to_list()
        new_values.append(value)

    # iterate permutations for all properties & values
    # first value is always the area
    for area in new_values[0]:
        # form conditions & select flows for current permutation
        conditions = [df_new[on[0]] == area]
        for key, val in zip(on[1:], new_values[1:]):
            conditions.append(df_new[key].isin(val))
        flows = df_new.loc[np.bitwise_and.reduce(conditions)]

        # save data for graphs
        # add individual data to RESULTS
        if add_graph:
            to_save = {}
            for year in var.YEARS:
                for period in range(1, len(periods) + 1):
                    if year < var.YEARS[-1] or period <= var.QUARTER:
                        amount = flows[(flows['MeldPeriodeJAAR'] == year) &
                                       (flows['Periode'] == period)]['Gewicht_TN']
                        amount = amount.values[0] if len(amount) else 0
                        key = f'{prop}\tQ{period}/{str(year)[-2:]}\tt'
                        to_save[key] = amount
            save(to_save, area=area)

        # run linear regression
        if add_trends:
            # prepare data
            X, Y = [], []
            for year in var.YEARS:
                for period in range(1, len(periods) + 1):
                    if year < var.YEARS[-1] or period <= var.QUARTER:
                        time = year * 12 + period * per_months
                        amount = flows[(flows['MeldPeriodeJAAR'] == year) &
                                       (flows['Periode'] == period)]['Gewicht_TN']
                        amount = amount.values[0] if len(amount) else 0
                        X.append(time)
                        Y.append(amount)
            X = np.array(X).reshape(-1, 1)

            # linear regression
            reg = LinearRegression().fit(X, Y)

            # compute initial & final amount based on model
            Y_initial = reg.predict(np.array(X[0]).reshape(-1, 1))[0]
            Y_final = reg.predict(np.array(X[-1]).reshape(-1, 1))[0]

            # overall change (tn)
            change = Y_final - Y_initial
            DATA.setdefault(f'{prop}\tall years\tt', {})[area] = to_json(change)

            # overall change (%)
            change = (Y_final - Y_initial) / abs(Y_initial) * 100 if Y_initial else np.nan
            DATA.setdefault(f'{prop}\tall years\t%', {})[area] = to_json(change)

            # change to same quarter, last year (tn)
            Y_final, Y_initial = Y[-1], Y[-5]
            change = Y_final - Y_initial
            DATA.setdefault(f'{prop}\tlast quarter\tt', {})[area] = to_json(change)

            # change to same quarter, last year (%)
            change = (Y_final - Y_initial) / Y_initial * 100 if Y_initial else np.nan
            DATA.setdefault(f'{prop}\tlast quarter\t%', {})[area] = to_json(change)


if __name__ == '__main__':
    """
    Compute actions for provincie & gemeenten
    """
    # get areas
    print("Load areas...\n")
    provincies, gemeenten = get_areas()

    print("INPUTS...")
    all_years = []
    for year in var.YEARS:
        print(f"Load {year} flows...")
        flows = get_flows(year=year)
        flows['Gewicht_TN'] = flows['Gewicht_KG'] / 10**3
        print(f"Total flows: {len(flows)}")

        # add gemeente & provincie to flow origins (herkomst)
        print("Add gemeente & provincie to flow origins (herkomst)...")
        flows = add_areas(flows, role='Herkomst', areas=gemeenten, admin_level='Gemeente')
        flows = add_areas(flows, role='Herkomst', areas=provincies, admin_level='Provincie')

        # add gemeente & provincie to flows destinations (verwerker)
        print("Add gemeente & provincie to flows destinations (verwerker)...")
        flows = add_areas(flows, role='Verwerker', areas=gemeenten, admin_level='Gemeente')
        flows = add_areas(flows, role='Verwerker', areas=provincies, admin_level='Provincie')

        # filter flows with origin (herkomst) or destination (verwerker)
        # within province in study
        print("Filter flows within province in study...")
        flows = flows.loc[
            (flows['Herkomst_Provincie'] == var.PROVINCE) |
            (flows['Verwerker_Provincie'] == var.PROVINCE)
        ]
        all_years.append(flows)
        print()
    print("Merge all years...")
    flows = pd.concat(all_years)
    print(f"Total flows: {len(flows)}\n")

    # import activities
    ACTIVITIES = pd.read_excel('data/flows/activitygroup.xlsx')
    ACTIVITIES['name'] = ACTIVITIES['name'].str.lower().str.capitalize()

    # import industries
    industries = pd.read_csv('./data/materials/ewc_industries.csv', low_memory=False, sep=';')
    industries['ewc'] = industries['ewc'].astype(str).str.zfill(6)
    flows['EuralCode'] = flows['EuralCode'].astype(str).str.zfill(6)
    flows = pd.merge(flows, industries, how='left', left_on='EuralCode', right_on='ewc')
    flows.loc[flows['industries'].isnull(), 'industries'] = 'Unknown'
    industry_groups = flows['industries'].drop_duplicates().to_list()

    # get names of provincie gemeenten
    provincie_gemeenten = gemeenten[gemeenten['parent'] == var.PROVINCE]['name'].to_list()

    # TRENDS (All amounts in tonnes)
    roles = [
        'Herkomst',
        # 'Verwerker'
    ]  # herkomst: production, verwerker: treatment
    levels = [
        'Provincie',
        # 'Gemeente'
    ]
    for role, level in itertools.product(roles, levels):
        on = f'{role}_{level}'

        terms = {
            'Herkomst': 'production',
            'Verwerker': 'treatment',
            'Provincie': 'province',
            'Gemeente': 'municipality'
        }
        prefix = f'{terms[level]}\t{terms[role]}'
        areas = [var.PROVINCE] if level == 'Provincie' else provincie_gemeenten

        # Average quarterly change on GENERAL waste
        compute_trends(flows,
                       on=[on],
                       values=[areas],
                       per_months=3, prop=f'{prefix}\ttotal\ttotal',
                       add_graph=False)

        # Average quarterly change in ECONOMIC ACTIVITIES
        # ONLY PRODUCTION
        # if terms[role] == 'production':
        for index, activity in ACTIVITIES.iterrows():
            compute_trends(flows,
                           on=[on, 'Ontdoener_AG'],
                           values=[areas, [activity['code']]],
                           per_months=3, prop=f'{prefix}\tactivity\t{activity["code"]} - {activity["name"]}',
                           add_graph=False)

        # Average quarterly change in TREATMENT METHODS
        # ONLY PRODUCTION
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
        # # if terms[role] == 'production':
        for method, codes in TREATMENT_METHODS.items():
            compute_trends(flows,
                           on=[on, 'VerwerkingsmethodeCode'],
                           values=[areas, codes],
                           per_months=3, prop=f'{prefix}\tprocessing\t{method}',
                           add_graph=False)

        # Average quarterly change in INDUSTRIES per TREATMENT method
        # ONLY PRODUCTION
        # # if terms[role] == 'production':
        for group in industry_groups:
            for method, codes in TREATMENT_METHODS.items():
                compute_trends(flows,
                               on=[on, 'industries', 'VerwerkingsmethodeCode'],
                               values=[areas, [group], codes],
                               per_months=3, prop=f'{prefix}\tindustry\t{group}_{method}',
                               add_trends=False)

    with open('test/actions.json', 'w') as outfile:
        fields = sorted(list(DATA.keys()))
        fields = zip(*[iter(fields)] * 2)

        for tup in fields:
            key = tup[0]
            values = DATA[key]
            level, field, category, typ, period, unit = key.split('\t')

            for value in values.items():
                name, amount = value
                waste = {
                    'percentage': {
                        'value': amount,
                        'unit': unit
                    }
                }
                for key in tup[1:]:
                    new_unit = key.split('\t')[-1]
                    waste['weight'] = {
                        'value': DATA[key][name],
                        'unit': new_unit
                    }

                RESULTS.setdefault(field, []).append({
                    'name': name,
                    'level': level,
                    'category': category,
                    'type': typ,
                    'period': period,
                    'values': {
                        'waste': waste
                    }
                })

        import _make_iterencode
        json.encoder._make_iterencode = _make_iterencode._make_iterencode
        indent = (2, None)
        json.dump(RESULTS, outfile, indent=indent)

