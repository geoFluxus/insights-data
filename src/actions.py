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
    if np.isnan(value): return None
    return round(value, 2)


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

    # quarter analysis
    quarter = var.QUARTER
    initial_year, final_year = var.YEARS[-2], var.YEARS[-1]

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
                    amount = flows[(flows['MeldPeriodeJAAR'] == year) &
                                   (flows['Periode'] == period)]['Gewicht_TN']
                    amount = amount.values[0] if len(amount) else 0
                    key = f'{prop}\t{year}-Q{period}\ttn'
                    to_save[key] = amount
            save(to_save, area=area)

        # run linear regression
        if add_trends:
            # prepare data
            X, Y = [], []
            for idx, flow in flows.iterrows():
                time = flow['MeldPeriodeJAAR'] * 12 + flow['Periode'] * per_months
                amount = flow['Gewicht_TN']
                X.append(time)
                Y.append(amount)
            X = np.array(X).reshape(-1, 1)

            Y_initial, Y_final = np.nan, np.nan
            if Y:
                # linear regression
                reg = LinearRegression().fit(X, Y)

                # compute initial & final amount based on model
                Y_initial = reg.predict(np.array(X[0]).reshape(-1, 1))[0]
                Y_final = reg.predict(np.array(X[-1]).reshape(-1, 1))[0]

            # overall change (tn)
            change = Y_final - Y_initial
            DATA.setdefault(f'{prop}\tall years\ttn', {})[area] = to_json(change)

            # overall change (%)
            change = (Y_final - Y_initial) / Y_initial * 100
            DATA.setdefault(f'{prop}\tall years\t%', {})[area] = to_json(change)

            # change to same quarter, last year (tn)
            Y_initial, Y_final = np.nan, np.nan
            if len(flows):
                Y_initial = flows[(flows['MeldPeriodeJAAR'] == initial_year) &
                                  (flows['Periode'] == quarter)]['Gewicht_TN']
                Y_initial = Y_initial.values[0] if len(Y_initial) else np.nan
                Y_final = flows[(flows['MeldPeriodeJAAR'] == final_year) &
                                (flows['Periode'] == quarter)]['Gewicht_TN']
                Y_final = Y_final.values[0] if len(Y_final) else np.nan
            change = Y_final - Y_initial
            DATA.setdefault(f'{prop}\tlast quarter\ttn', {})[area] = to_json(change)

            # change to same quarter, last year (%)
            change = (Y_final - Y_initial) / Y_initial * 100
            DATA.setdefault(f'{prop}\tlast quarter\t%', {})[area] = to_json(change)


def compute_actions(flows, provincies, gemeenten):
    """
    Compute actions for provincie & gemeenten
    """

    # add gemeente & provincie to flow origins (herkomst)
    logging.info("Add gemeente & provincie to flow origins (herkomst)...")
    flows = add_areas(flows, role='Herkomst', areas=gemeenten, admin_level='Gemeente')
    flows = add_areas(flows, role='Herkomst', areas=provincies, admin_level='Provincie')

    # add gemeente & provincie to flows destinations (verwerker)
    logging.info("Add gemeente & provincie to flows destinations (verwerker)...")
    flows = add_areas(flows, role='Verwerker', areas=gemeenten, admin_level='Gemeente')
    flows = add_areas(flows, role='Verwerker', areas=provincies, admin_level='Provincie')

    # filter flows with origin (herkomst) or destination (verwerker)
    # within province in study
    logging.info("Filter flows within province in study...")
    flows = flows.loc[
        (flows['Herkomst_Provincie'] == var.PROVINCE) |
        (flows['Verwerker_Provincie'] == var.PROVINCE)
    ]

    # import industries
    industries = pd.read_csv('./data/materials/ewc_industries.csv', low_memory=False, sep=';')
    industries['ewc'] = industries['ewc'].astype(str).str.zfill(6)
    flows['EuralCode'] = flows['EuralCode'].astype(str).str.zfill(6)
    flows = pd.merge(flows, industries, how='left', left_on='EuralCode', right_on='ewc')
    flows.loc[flows['industry'].isnull(), 'industry'] = 'Unknown'
    industry_groups = flows['industry'].drop_duplicates().to_list()

    # get names of provincie gemeenten
    provincie_gemeenten = gemeenten[gemeenten['parent'] == var.PROVINCE]['name'].to_list()

    # TRENDS (All amounts in tonnes)
    roles = ['Herkomst', 'Verwerker']  # herkomst: production, verwerker: treatment
    levels = ['Provincie', 'Gemeente']
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
        if terms[role] == 'production':
            for method, codes in TREATMENT_METHODS.items():
                compute_trends(flows,
                               on=[on, 'VerwerkingsmethodeCode'],
                               values=[areas, codes],
                               per_months=3, prop=f'{prefix}\tprocess\t{method}',
                               add_graph=False)

        # Average quarterly change in INDUSTRY GROUPS per TREATMENT METHOD
        # ONLY PRODUCTION
        if terms[role] == 'production':
            for method, codes in TREATMENT_METHODS.items():
                for group in industry_groups:
                    compute_trends(flows,
                                   on=[on, 'VerwerkingsmethodeCode', 'industry'],
                                   values=[areas, codes, [group]],
                                   per_months=3, prop=f'{prefix}\tmaterial\t{method}_{group}',
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

