import variables as var
import pandas as pd
import geopandas as gpd
from sklearn.linear_model import LinearRegression
import numpy as np
import logging
import itertools
import json


DATA = {}


def to_none(value):
    if np.isnan(value): return None
    return value


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


def compute_trends(df, on=[], values=[], per_months=12, prop=None):
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
    for value in itertools.product(*new_values):
        area = value[0]

        # form conditions & select flows for current permutation
        conditions = []
        for key, val in zip(on, value):
            conditions.append(df_new[key] == val)
        flows = df_new.loc[np.bitwise_and.reduce(conditions)]

        # prepare data & run linear regression
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
        DATA.setdefault(f'{prop}\t{var.YEARS[0]}-{var.YEARS[-1]}\ttn', {})[area] = to_none(change)

        # overall change (%)
        change = (Y_final - Y_initial) / Y_initial * 100
        DATA.setdefault(f'{prop}\t{var.YEARS[0]}-{var.YEARS[-1]}\tpercentage', {})[area] = to_none(change)

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
        DATA.setdefault(f'{prop}\t{var.YEARS[-1]}-Q{quarter}\ttn', {})[area] = to_none(change)

        # change to same quarter, last year (%)
        change = (Y_final - Y_initial) / Y_initial * 100
        DATA.setdefault(f'{prop}\t{var.YEARS[-1]}-Q{quarter}\tpercentage', {})[area] = to_none(change)


def compute_actions(flows, provincies, gemeenten):
    """
    Compute actions for provincie & gemeenten
    """

    # # add gemeente & provincie to flow origins (herkomst)
    # logging.info("Add gemeente & provincie to flow origins (herkomst)...")
    # flows = add_areas(flows, role='Herkomst', areas=gemeenten, admin_level='Gemeente')
    # flows = add_areas(flows, role='Herkomst', areas=provincies, admin_level='Provincie')
    #
    # # add gemeente & provincie to flows destinations (verwerker)
    # logging.info("Add gemeente & provincie to flows destinations (verwerker)...")
    # flows = add_areas(flows, role='Verwerker', areas=gemeenten, admin_level='Gemeente')
    # flows = add_areas(flows, role='Verwerker', areas=provincies, admin_level='Provincie')
    #
    # flows.to_csv('utrecht_actions.csv', index=False)

    # filter flows with origin (herkomst) or destination (verwerker)
    # within province in study
    logging.info("Filter flows within province in study...")
    flows = flows.loc[
        (flows['Herkomst_Provincie'] == var.PROVINCE) |
        (flows['Verwerker_Provincie'] == var.PROVINCE)
    ]

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
                       per_months=3, prop=f'{prefix}_general')

        # Average quarterly change in LANDFILL change:
        ewc = ['G01', 'G02']
        compute_trends(flows,
                       on=[on, 'VerwerkingsmethodeCode'],
                       values=[areas, ewc],
                       per_months=3, prop=f'{prefix}_landfill')

        # Average quarterly change in INCINERATION change:
        ewc = ['B04', 'F01', 'F02', 'F06', 'F07']
        compute_trends(flows,
                       on=[on, 'VerwerkingsmethodeCode'],
                       values=[areas, ewc],
                       per_months=3, prop=f'{prefix}_incineration')

        # Average quarterly change in REUSE change:
        ewc = ['B01', 'B03', 'B05']
        compute_trends(flows,
                       on=[on, 'VerwerkingsmethodeCode'],
                       values=[areas, ewc],
                       per_months=3, prop=f'{prefix}_reuse')

        # Average quarterly change in RECYCLING change:
        ewc = ['C01', 'C02', 'C03', 'C04', 'D01',
               'D02', 'D03', 'D04', 'D05', 'D06',
               'E01', 'E02', 'E03', 'E04', 'E05',
               'F03', 'F04']
        compute_trends(flows,
                       on=[on, 'VerwerkingsmethodeCode'],
                       values=[areas, ewc],
                       per_months=3, prop=f'{prefix}_recycling')

        # Average quarterly change in STORAGE change:
        ewc = ['A01', 'A02']
        compute_trends(flows,
                       on=[on, 'VerwerkingsmethodeCode'],
                       values=[areas, ewc],
                       per_months=3, prop=f'{prefix}_storage')

    res = {}
    with open('test/actions.json', 'w') as outfile:
        fields = sorted(list(DATA.keys()))
        fields = zip(*[iter(fields)] * 2)

        for tup in fields:
            key = tup[0]
            values = DATA[key]
            level, field, period, unit = key.split('\t')

            for value in values.items():
                name, amount = value
                waste = {
                    unit: amount
                }
                for key in tup[1:]:
                    new_unit = key.split('\t')[-1]
                    waste[new_unit] = DATA[key][name]

                res.setdefault(field, []).append({
                    'name': name,
                    'level': level,
                    'period': period,
                    'values': {
                        'waste': waste
                    }
                })

        json.dump(res, outfile, indent=4)

