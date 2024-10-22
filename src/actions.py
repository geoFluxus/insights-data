import variables as var
import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np
import json
import itertools
import re
import utils
import benchmark


VARS = {
    'INPUT_DIR': var.INPUT_DIR,
    'AREA_DIR': var.AREA_DIR,
    'AREA': var.AREA,
    'LEVEL': var.LEVEL,
    'YEARS': var.ACTIONS_YEARS,
    'QUARTER': var.QUARTER,
    'OUTPUT_DIR': var.OUTPUT_DIR
}


AREA = VARS['AREA']
LEVEL = VARS['LEVEL']
ALL_YEARS = "all_years"
LAST_QUARTER = "last_quarter"
UNKNOWN = 'Onbekend'
DATA = {}


def to_json(value):
    if np.isnan(value): return 0
    return value


def get_flows(year=None):
    """
    Import flows from 'data/flows'
    """
    path = f"{VARS['INPUT_DIR']}/{VARS['AREA_DIR']}/LMA/processed"
    filename = f"{path}/ontvangst_{VARS['AREA'].lower()}_{year}_full.csv"

    return pd.read_csv(filename, low_memory=False)


def save(flows, datatype=None, prop=None, unit='t'):
    X, Y = [], []
    for flow in flows:
        X.append(flow['period'])
        Y.append(to_json(flow['amount']))
    item = DATA.setdefault(datatype, {})
    item[prop] = {
        'name': AREA,
        'level': LEVEL,
        'period': X,
        'value': Y,
        'unit': unit
    }


def compute_trends(df, on=[], values=[], datatype=None, prop=None,
                   per_months=12, add_graph=True, add_trends=True):
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
    for year in VARS['YEARS']:
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
        flows = flows[['MeldPeriodeJAAR', 'Periode', 'Gewicht_TN']]\
            .groupby(['MeldPeriodeJAAR', 'Periode']).sum().reset_index()

        # save data for graphs
        # add individual data to RESULTS
        if add_graph:
            to_save = []
            for year in VARS['YEARS']:
                for period in range(1, len(periods) + 1):
                    if year < VARS['YEARS'][-1] or period <= VARS['QUARTER']:
                        amount = flows[(flows['MeldPeriodeJAAR'] == year) &
                                       (flows['Periode'] == period)]['Gewicht_TN']
                        amount = amount.values[0] if len(amount) else 0
                        to_save.append({
                            'amount': amount,
                            'period': f'Q{period}/{str(year)[-2:]}'
                        })
            save(to_save, datatype=datatype, prop=prop)

        # run linear regression
        if add_trends:
            # prepare data
            X, Y = [], []
            for year in VARS['YEARS']:
                for period in range(1, len(periods) + 1):
                    if year < VARS['YEARS'][-1] or period <= VARS['QUARTER']:
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
            # add regression for production graph
            if datatype == 'production_graph':
                DATA[datatype]['line'] = {
                    'y1': Y_initial,
                    'y2': Y_final,
                }
                return

            # overall change)
            amount_change = (Y_final - Y_initial) / len(VARS['YEARS'])
            pct_change = ((Y_final - Y_initial) / abs(Y_initial)) / len(VARS['YEARS']) * 100 \
                if Y_initial else np.nan
            item = DATA.setdefault(datatype, {})
            item[ALL_YEARS] = {
                'amount': to_json(amount_change),
                'unit': 't',
                'pct': to_json(pct_change),
                'period': str(VARS['YEARS'][0])
            }

            # change to same quarter, last year
            Y_final, Y_initial = Y[-1], Y[-5]
            amount_change = Y_final - Y_initial
            pct_change = (Y_final - Y_initial) / Y_initial * 100 if Y_initial else np.nan
            item[LAST_QUARTER] = {
                'amount': to_json(amount_change),
                'unit': 't',
                'pct': to_json(pct_change),
                'period': f"Q{VARS['QUARTER']}/{str(VARS['YEARS'][-2])[-2:]}"
            }


if __name__ == '__main__':
    """
    Compute actions for provincie & gemeenten
    """
    ROLES = var.ROLES
    PREFIXES = var.PREFIXES
    TREATMENT_METHODS = var.TREATMENT_METHODS

    # start analysis
    print('ACTIONS ANALYSIS')
    print('VARIABLES:')
    for name, value in VARS.items():
        print(f'{name}={value}')

    # import areas
    # import province polygon
    polygon = utils.import_areas(level=VARS['LEVEL'])
    polygon = polygon[polygon['name'] == VARS['AREA']]
    assert len(polygon) == 1

    # start analysis
    all_years = []
    for year in VARS['YEARS']:
        print(f"\nLoad {year} flows...")
        flows = get_flows(year=year)
        flows['Gewicht_TN'] = flows['Gewicht_KG'] / 10**3
        print(f"Total flows: {len(flows)}")

        # add areas to roles
        source = ROLES['Ontvangst']['source']  # source role
        flows = utils.add_areas(flows, role=source, areas=polygon, admin_level=LEVEL)

        all_years.append(flows)

    print("Merge all years...")
    flows = pd.concat(all_years)
    print(f"Total flows: {len(flows)}\n")

    # import industries
    industries = pd.read_csv(f"{VARS['INPUT_DIR']}/DATA/ontology/ewc_industries.csv", low_memory=False, sep=';')
    industries['ewc'] = industries['ewc'].astype(str).str.zfill(6)
    flows['EuralCode'] = flows['EuralCode'].astype(str).str.zfill(6)
    flows = pd.merge(flows, industries, how='left', left_on='EuralCode', right_on='ewc')
    flows.loc[flows['industries'].isnull(), 'industries'] = UNKNOWN
    industry_groups = flows['industries'].drop_duplicates().to_list()

    # TRENDS (All amounts in tonnes) -> ONLY PRODUCTION
    on = f'Herkomst_{LEVEL}'

    # average quarterly change on GENERAL waste
    compute_trends(flows,
                   on=[on],
                   values=[[AREA]],
                   per_months=3,
                   datatype='production_trends',
                   prop='total',
                   add_graph=False)

    # average quarterly change on GENERAL waste
    compute_trends(flows,
                   on=[on],
                   values=[[AREA]],
                   per_months=3,
                   datatype='production_graph',
                   prop='points',)

    # average quarterly change in INDUSTRIES per TREATMENT method
    for group in industry_groups:
        for method, codes in TREATMENT_METHODS.items():
            formatted_name = " ".join(
                re.findall('[A-Z][^A-Z]*',
                           group.replace('Industrie', '')
                                .replace('Industry', '')
                           )
            )
            prop = f'{formatted_name}_{method}'.lower().replace(' ', '_')
            compute_trends(flows,
                           on=[on, 'industries', 'VerwerkingsmethodeCode'],
                           values=[[AREA], [group], codes],
                           per_months=3,
                           datatype='process_trends',
                           prop=prop,
                           add_trends=False)

    # eural treemap
    DATA['eural_treemap'] = benchmark.treemap()

    with open(f"{VARS['OUTPUT_DIR']}/actions.json", 'w') as outfile:
        from src import _make_iterencode
        json.encoder._make_iterencode = _make_iterencode._make_iterencode
        indent = (2, None)
        json.dump(DATA, outfile, indent=indent)

