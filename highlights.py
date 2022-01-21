import pandas as pd
import numpy as np
import utils
import variables as var
import matplotlib.pyplot as plot


VARS = {
    'INPUT_DIR': var.INPUT_DIR,
    'AREA': var.AREA,
    'LEVEL': var.LEVEL,
    'POSTCODES': var.POSTCODES,
    'YEAR': var.YEAR,
    'OUTPUT_DIR': var.OUTPUT_DIR,
    'COMPANY_WASTE_UNIT': var.UNITS['HIGHLIGHTS']['COMPANY_WASTE']
}

# # INPUTS
# PROVINCE = "Utrecht"
# YEAR = 2019
# COROPS = [
#     'Utrecht'
# ]


def to_dec(num):
    return round(num, 1)


def import_household_data(areas=None):
    """
    Import & ready CBS household data for processing
    """

    # add gemeente & provincie
    df = pd.read_excel(f"{VARS['INPUT_DIR']}/{VARS['AREA']}/CBS/Huishoudelijk_Gemeenten.xlsx", sheet_name='Data')
    columns = list(df.columns)
    df = df.replace('?', np.nan)
    df = pd.merge(df, areas, left_on='Gebieden', right_on='Gemeente', how='left')
    columns.append('Provincie')
    df = df[columns]

    # import population data
    population = pd.read_csv(f"{VARS['INPUT_DIR']}/{VARS['AREA']}/CBS/populationNL.csv", delimiter=';')

    # add population
    def add_population(row):
        gemeente, year = row['Gebieden'], row['Perioden']
        res = population[population['Gemeente'] == gemeente][str(year)]
        if not res.empty:
            res = res.values[0]
            return res
        return np.nan
    df['Inwoners'] = df.apply(lambda x: add_population(x), axis=1)

    return df


def cbs_primary_waste(input):
    df = input.copy()
    df['Gewicht_KG'] = df["Totaal aangeboden huishoudelijk afval [Kilo's per inwoner]"] * df['Inwoners']
    result = df['Gewicht_KG'].sum()

    return result


if __name__ == "__main__":
    ROLES = var.ROLES

    # start analysis
    print('HIGHLIGHTS ANALYSIS')
    print('VARIABLES:')
    for name, value in VARS.items():
        print(f'{name}={value}')

    # import postcodes
    # import postcodes
    postcodes = pd.read_csv(f"{VARS['INPUT_DIR']}/GEODATA/postcodes/{VARS['POSTCODES']}.csv", low_memory=False)
    postcodes['PC4'] = postcodes['PC4'].astype(str)
    gemeenten = postcodes[['Gemeente', 'Provincie']].drop_duplicates()
    area_gemeenten = gemeenten[gemeenten[f"{VARS['LEVEL']}"] == VARS['AREA']]['Gemeente'].to_list()
    print(f'AREA GEMEENTEN ({len(area_gemeenten)}): {sorted(area_gemeenten)}\n')

    # import province polygon
    polygon = utils.import_areas(level=VARS['LEVEL'])
    polygon = polygon[polygon['name'] == VARS['AREA']]
    assert len(polygon) == 1

    # import CBS household data
    print('Import CBS household data...\n')
    CBS = import_household_data(areas=gemeenten)
    CBS = CBS.rename(columns={'Gebieden': 'Gemeente'})
    CBS = CBS[
        (CBS['Provincie'] == VARS['AREA']) &
        (CBS['Perioden'] == VARS['YEAR'])
    ]

    # import LMA data
    print('Import LMA Ontvangst...')
    typ = 'Ontvangst'
    path = f"{VARS['INPUT_DIR']}/{VARS['AREA']}/LMA/processed"
    filename = f"{path}/{typ.lower()}_{VARS['AREA'].lower()}_{VARS['YEAR']}.csv"
    LMA = pd.read_csv(filename, low_memory=False)
    # add areas to roles
    print('Add areas to roles...')
    source = ROLES[typ]['source']  # source role
    target = ROLES[typ]['target']  # target role
    for role in [source, target]:
        LMA = utils.add_areas(LMA,
                             areas=polygon,
                             role=role,
                             admin_level='Provincie')

    # HIGHLIGHTS
    print('HIGHLIGHTS')

    # waste produced by companies
    lma = LMA.copy()
    lma['EC2'] = lma['EuralCode'].astype(str).str.zfill(6).str[:2]
    lma = lma[
        (lma['Herkomst_Provincie'] == VARS['AREA']) &
        (lma['EC2'] != '19')
    ]  # all waste produced except chapter 19
    company_amount = lma['Gewicht_KG'].sum()
    total_amount = company_amount + cbs_primary_waste(CBS)
    perc = company_amount / total_amount * 100
    print(f"{to_dec(perc)}% "
          f"({utils.kg_to_unit(company_amount, unit=VARS['COMPANY_WASTE_UNIT'])} {VARS['COMPANY_WASTE_UNIT']}) "
          f"was produced by companies")

    # X% of waste produced by X% of companies
    print()
    lma = LMA.copy()
    lma['EC2'] = lma['EuralCode'].astype(str).str.zfill(6).str[:2]
    lma = lma[
        (lma['Herkomst_Provincie'] == VARS['AREA']) &
        (lma['EC2'] != '19')
    ]  # all waste produced except chapter 19
    groupby = [
        'Ontdoener',
        'Gewicht_KG'
    ]
    companies = lma[groupby].groupby(groupby[:-1]).sum().reset_index()
    companies = companies.sort_values(by=groupby[-1], ascending=False)
    bar_num = 100
    idx = int(len(companies) / bar_num)
    to_plot = companies[companies.index % idx == 0]
    ax = to_plot[:100].plot.bar(x=groupby[0], y=groupby[1], rot=0)
    plot.show()

    print('Enter company percent:')
    company_perc = int(input())
    idx = int(len(companies) * company_perc / 100)
    # print(companies[:100].to_csv('greatest_producers.csv', index=False))
    company_amount = companies[:idx]['Gewicht_KG'].sum()
    amount_perc = company_amount / companies['Gewicht_KG'].sum() * 100
    print(f"{to_dec(amount_perc)}% "
          f"({utils.kg_to_unit(company_amount, unit=VARS['COMPANY_WASTE_UNIT'])} {VARS['COMPANY_WASTE_UNIT']}) "
          f" of company waste produced by "
          f"{company_perc}% of companies")




