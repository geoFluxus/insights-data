import pandas as pd
import numpy as np
import utils
import variables as var
import matplotlib.pyplot as plot


# INPUTS
PROVINCE = "Utrecht"
YEAR = 2019
COROPS = [
    'Utrecht'
]


def to_dec(num):
    return round(num, 1)


def import_household_data(areas=None):
    """
    Import & ready CBS household data for processing
    """

    # add gemeente & provincie
    df = pd.read_excel('./data/household/Huishoudelijk_Gemeenten.xlsx', sheet_name='Data')
    columns = list(df.columns)
    df = df.replace('?', np.nan)
    df = pd.merge(df, areas, left_on='Gebieden', right_on='Gemeente (post 2019)', how='left')
    columns.append('Provincie')
    df = df[columns]

    # import population data
    population = pd.read_csv('./data/areas/populationNL.csv', delimiter=';')

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
    print(f'YEAR: {YEAR}\n')
    ROLES = var.ROLES

    # import province polygon
    polygon = utils.import_areas(level='provincies')
    polygon = polygon[polygon['name'] == PROVINCE]
    assert len(polygon) == 1

    # import postcodes
    postcodes = pd.read_excel('./data/areas/postcodesNL.xlsx')
    postcodes['PC4'] = postcodes['PC4'].astype(str)
    gemeenten = postcodes[['Gemeente (post 2019)', 'Provincie']].drop_duplicates()
    provincie_gemeenten = gemeenten[gemeenten['Provincie'] == PROVINCE]['Gemeente (post 2019)'].to_list()
    print(f'PROVINCIE GEMEENTEN ({len(provincie_gemeenten)}): {sorted(provincie_gemeenten)}\n')

    # import CBS household data
    print('Import CBS household data...\n')
    CBS = import_household_data(areas=gemeenten)
    CBS = CBS.rename(columns={'Gebieden': 'Gemeente'})
    CBS = CBS[
        (CBS['Provincie'] == PROVINCE) &
        (CBS['Perioden'] == YEAR)
    ]

    # import LMA data
    print('Import LMA Ontvangst...\n')
    typ = 'Ontvangst'
    path = f'../../../../../media/geofluxus/DATA/national/{PROVINCE.lower()}/processed'
    filename = f'{path}/{typ.lower()}_{PROVINCE.lower()}_{YEAR}.csv'
    LMA = pd.read_csv(filename, low_memory=False)
    # add areas to roles
    print('Add areas to roles...\n')
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
        (lma['Herkomst_Provincie'] == PROVINCE) &
        (lma['EC2'] != '19')
    ]  # all waste produced except chapter 19
    company_amount = lma['Gewicht_KG'].sum()
    total_amount = company_amount + cbs_primary_waste(CBS)
    perc = company_amount / total_amount * 100
    print(f'{to_dec(perc)}% '
          f'({to_dec(company_amount / 10**9)}Mt) '
          f'was produced by companies')

    # X% of waste produced by X% of companies
    print()
    lma = LMA.copy()
    lma['EC2'] = lma['EuralCode'].astype(str).str.zfill(6).str[:2]
    lma = lma[
        (lma['Herkomst_Provincie'] == PROVINCE) &
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
    print(f'{to_dec(amount_perc)}% '
          f'({to_dec(company_amount / 10**9)}Mt)'
          f' of company waste produced by '
          f'{company_perc}% of companies')




