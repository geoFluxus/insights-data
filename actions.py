import variables as var
import pandas as pd
import geopandas as gpd
from sklearn.linear_model import LinearRegression
import numpy as np
import itertools
import json
import itertools
import utils


# INPUTS
PROVINCE = 'Utrecht'
YEARS = [2016, 2017, 2018, 2019, 2020, 2021]
QUARTER = 2

DATA = {}

RESULTS = {}


def to_json(value):
    if np.isnan(value): return 0
    return round(value, 2)


def get_flows(year=None):
    """
    Import flows from 'data/flows'
    """
    path = f'../../../../../media/geofluxus/DATA/national/{PROVINCE.lower()}/processed'
    filename = f'{path}/ontvangst_{PROVINCE.lower()}_{year}.csv'

    return pd.read_csv(filename, low_memory=False)


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
    for year in YEARS:
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
            for year in YEARS:
                for period in range(1, len(periods) + 1):
                    if year < YEARS[-1] or period <= QUARTER:
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
            for year in YEARS:
                for period in range(1, len(periods) + 1):
                    if year < YEARS[-1] or period <= QUARTER:
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
    ROLES = var.ROLES
    PREFIXES = var.PREFIXES
    TREATMENT_METHODS = var.TREATMENT_METHODS
    LEVELS = [
        'Provincie',
        # 'Gemeente'
    ]

    # import areas
    AREAS = {}
    print("Import areas...\n")
    for level in LEVELS:
        name = 'provincies' if level == 'Provincie' else 'gemeenten'
        AREAS[level] = utils.import_areas(level=name)

    # start analysis
    all_years = []
    for year in YEARS:
        print()
        print(f"Load {year} flows...")
        flows = get_flows(year=year)
        flows['Gewicht_TN'] = flows['Gewicht_KG'] / 10**3
        print(f"Total flows: {len(flows)}")

        # add areas to roles
        source = ROLES['Ontvangst']['source']  # source role
        target = ROLES['Ontvangst']['target']  # target role
        for role, level in itertools.product([source, target], LEVELS):
            flows = utils.add_areas(flows, role=role, areas=AREAS[level], admin_level=level)

        all_years.append(flows)

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
    if 'gemeenten' in LEVELS:
        gemeenten = AREAS['gemeenten']
        provincie_gemeenten = gemeenten[gemeenten['parent'] == PROVINCE]['name'].to_list()

    # TRENDS (All amounts in tonnes) -> ONLY PRODUCTION
    for role, level in itertools.product(['Herkomst'], LEVELS):
        on = f'{role}_{level}'
        prefix = f'{PREFIXES[level]}\t{PREFIXES[role]}'
        areas = [PROVINCE] if level == 'Provincie' else provincie_gemeenten

        # average quarterly change on GENERAL waste
        compute_trends(flows,
                       on=[on],
                       values=[areas],
                       per_months=3, prop=f'{prefix}\ttotal\ttotal',
                       add_graph=False)

        # average quarterly change in ECONOMIC ACTIVITIES
        for index, activity in ACTIVITIES.iterrows():
            compute_trends(flows,
                           on=[on, 'Ontdoener_AG'],
                           values=[areas, [activity['code']]],
                           per_months=3, prop=f'{prefix}\tactivity\t{activity["code"]} - {activity["name"]}',
                           add_graph=False)

        # average quarterly change in TREATMENT METHODS
        for method, codes in TREATMENT_METHODS.items():
            compute_trends(flows,
                           on=[on, 'VerwerkingsmethodeCode'],
                           values=[areas, codes],
                           per_months=3, prop=f'{prefix}\tprocessing\t{method}',
                           add_graph=False)

        # average quarterly change in INDUSTRIES per TREATMENT method
        for group in industry_groups:
            for method, codes in TREATMENT_METHODS.items():
                compute_trends(flows,
                               on=[on, 'industries', 'VerwerkingsmethodeCode'],
                               values=[areas, [group], codes],
                               per_months=3, prop=f'{prefix}\tindustry\t{group}_{method}',
                               add_trends=False)

    with open('./test/actions.json', 'w') as outfile:
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

