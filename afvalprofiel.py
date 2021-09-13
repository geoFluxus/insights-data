import pandas as pd
import geopandas as gpd
import numpy as np


YEAR = 2019


def add_areas(flows, areas=None, role=None, admin_level=None):
    columns = list(flows)

    # format postcodes
    flows['PC4'] = flows[f'{role}_Postcode'].astype(str).str[:4]
    areas['PC4'] = areas['PC4'].astype(str)

    # merge
    flows = pd.merge(flows, areas[['PC4', admin_level]], how='left', on='PC4')

    # keep new column
    flows[f'{role}_{admin_level}'] = flows[admin_level]
    columns.append(f'{role}_{admin_level}')
    flows = flows[columns]

    return flows


if __name__=='__main__':
    # # import ontvangst (2019)
    # print('Import flows...')
    # path = f'../../../../../media/geofluxus/DATA/national/ontvangst/processed'
    # df = pd.read_csv(f'{path}/ontvangst_{YEAR}_full.csv', low_memory=False)
    #
    # # import provinces
    # print('Import provinces...')
    # postcodes = pd.read_excel('data/areas/postcodesNL.xlsx')
    #
    # # add province to producers (herkomst)
    # print('Add province to producers...')
    # df = add_areas(df, areas=postcodes, role='Herkomst', admin_level="Provincie")
    # print(df)
    #
    # # filter Noord-Holland
    # print('Filter producers in Noord-Holland...')
    # print(f'Initial flows: {len(df)}')
    # df = df[df['Herkomst_Provincie'] == 'Noord-Holland']
    # print(f'Final flows: {len(df)}')
    #
    # # export
    # print('Export...')
    # df.to_csv(f'test/noord_holland_producers.csv', index=False)

    # import Noord-Holland producers
    df = pd.read_csv('test/noord_holland_producers.csv', low_memory=False)

    # import study companies
    companies = pd.read_excel('test/Companies_Afvalprofiel.xlsx')
    companies['Bedrijf'] = companies['Bedrijf'].str.strip()
    companies['Bedrijf'] = companies['Bedrijf'].str.upper()
    companies['Plaats'] = companies['Plaats'].str.upper()
    df['Ontdoener'] = df['Ontdoener'].str.upper()

    concats = []
    columns = [
        'Ontdoener',
        'EuralCode',
        'VerwerkingsmethodeCode',
        'VerwerkingsOmschrijving',
        'BenamingAfval',
        'EURAL_gemengd/puur',
        'EURAL_schoon/vervuild',
        'EURAL_(an)organisch',
        'EURAL_(a)biotisch',
        'Gewicht_KG'
    ]
    for idx, row in companies.iterrows():
        if type(row['Bedrijf']) == str and row['Eligible']:
            match = df[(df['Ontdoener'].str.contains(row['Bedrijf'])) & (df['Herkomst_Plaats'] == row['Plaats'])]
            match = match[columns]
            match['Ontdoener'] = row['Bedrijf']
            concats.append(match)

    results = pd.concat(concats)
    results = results.drop_duplicates()
    results = results.groupby(columns[:-1]).sum().reset_index()
    results.to_excel('test/companies_afvalprofiel_ontvangst_2019.xlsx', index=False)