import pandas as pd
import logging
import numpy as np
from memory_profiler import profile


INWONERS = {
    'Groningen': 586813,
    'Friesland': 651459,
    'Drenthe': 494760,
    'Overijssel': 1166478,
    'Flevoland': 428264,
    'Gelderland': 2096620,
    'Utrecht': 1361093,
    'Noord-Holland': 2887906,
    'Zuid-Holland': 3726173,
    'Zeeland': 385379,
    'Noord-Brabant': 2573853,
    'Limburg': 1115895
}


def import_household_data(areas=None):
    """
    Import & ready CBS household data for processing
    """
    df = pd.read_excel('./data/household/Huishoudelijk_Gemeenten_Utrecht.xlsx', sheet_name='Data')
    columns = list(df.columns)
    df = df.replace('?', np.nan)
    df = pd.merge(df, areas, left_on='Gebieden', right_on='Gemeente', how='left')
    columns.append('Provincie')
    df = df[columns]
    missing = {
        'Altena': 'Noord-Brabant',
        'Beekdaelen': 'Limburg',
        'Den Haag': 'Zuid-Holland',
        'Het Hogeland': 'Groningen',
        'Hoeksche Waard': 'Zuid-Holland',
        'Midden-Groningen': 'Groningen',
        'Molenlanden': 'Zuid-Holland',
        'Noardeast-Fryslân': 'Friesland',
        'Nuenen c.a.': 'Noord-Brabant',
        'Vijfheerenlanden': 'Utrecht',
        'Waadhoeke': 'Friesland',
        'West Betuwe': 'Gelderland',
        'Westerkwartier': 'Groningen',
        'Westerwolde': 'Groningen'
    }
    for key, value in missing.items():
        df.loc[df['Gebieden'] == key, 'Provincie'] = value

    return df


def add_areas(df, areas=None, role=None):
    """
    Add gemeente & provincie to role based on postcode
    """
    columns = list(df)

    # join on postcode
    df = pd.merge(df, areas, left_on=f'{role}_PC4', right_on='PC4', how='left')
    df[f'{role}_Gemeente'] = df['Gemeente']
    df[f'{role}_Provincie'] = df['Provincie']
    columns.extend([f'{role}_Gemeente', f'{role}_Provincie'])
    df = df[columns]

    return df

@profile
def import_lma_flows(areas=None, year=None):
    """
    Import & ready LMA flows for processing
    """
    columns = [
        'EuralCode',
        'VerwerkingsmethodeCode',
        'Gewicht_KG',
        'Herkomst_Postcode',
        'Herkomst_Land',
        'Verwerker_Postcode',
        'Verwerker_Land'
    ]

    # import file
    path = '../../../../../media/geofluxus/DATA/national/ontvangst/processed'
    print(f'Import {year} data...')
    df = pd.read_csv(f'{path}/ontvangst_{year}_full.csv', usecols=columns, low_memory=False)

    # ewc code as 6-digit
    df['EuralCode'] = df['EuralCode'].astype(str).str.zfill(6)

    # add provincie/gemeente on herkomst & verwerker
    for role in ['Herkomst', 'Verwerker']:
        df.loc[df[f'{role}_Land'] == 'NEDERLAND', f'{role}_PC4'] = df[f'{role}_Postcode'].str[:4]
        df = add_areas(df, areas=areas, role=role)

    return df


def compute_lma_waste(df, role=None, apply=None, text=None):
    """
    Compute primary waste (except chapter 19)
    """
    print(text)

    columns = [
        f'{role}_Provincie',
        'Gewicht_KG'
    ]

    # apply filter function
    if apply: df = apply(df)

    # compute total
    df = df[columns].groupby(columns[:-1]).sum().reset_index()
    logging.info(f'{text} - {role} (Mtn)')
    for idx, row in df.iterrows():
        provincie = row[f'{role}_Provincie']
        amount = row[f'Gewicht_KG'] / 10**9
        logging.info(f'{provincie}: {amount}')
    logging.info('\n')


def cbs_primary_waste(df):
    columns = [
        "Provincie",
        "Totaal aangeboden huishoudelijk afval [Kilo's per inwoner]"
    ]
    df = df[columns].groupby(columns[:-1]).mean().reset_index()
    logging.info(f'Primary Waste - Household (Mtn)')
    for key, value in INWONERS.items():
        df.loc[df['Provincie'] == key, 'Inwoners'] = value
        df['Gewicht_KG'] = df["Totaal aangeboden huishoudelijk afval [Kilo's per inwoner]"] * df['Inwoners']
    for idx, row in df.iterrows():
        provincie = row[f'Provincie']
        amount = row[f'Gewicht_KG'] / 10 ** 9
        logging.info(f'{provincie}: {amount}')
    logging.info('\n')


if __name__ == '__main__':
    logging.basicConfig(filename="logs.txt",  # file name
                        filemode="w",  # overwrite
                        level=logging.INFO, # lowest warning level
                        format="%(message)s"
                        )

    # import postcodes
    print('Import household data...\n')
    postcodes = pd.read_excel('./data/areas/postcodesNL.xlsx')
    postcodes['PC4'] = postcodes['PC4'].astype(str)
    gemeenten = postcodes[['Gemeente', 'Provincie']].drop_duplicates()

    # # import household data
    # household_data = import_household_data(areas=gemeenten)

    # import national ontvangst (2016-2019)
    years = ['2016', '2017', '2018', '2019']
    for year in years:
        print(f'Analyse {year}...')
        logging.info(f'ANALYSIS YEAR: {year}\n')
        lma_flows = import_lma_flows(areas=postcodes, year=year)
        del lma_flows
        # cbs_flows = household_data[household_data['Perioden'] == int(year)]
        #
        # # total primary waste
        # text = 'Total primary waste'
        # def primary_waste(df):
        #     return df[df['EuralCode'].str[:2] != '19']
        # compute_lma_waste(lma_flows, role='Herkomst', apply=primary_waste, text=text)
        # # cbs_primary_waste(cbs_flows)
        #
        # # incineration waste
        # text = 'Incineration waste'
        # def incineration_waste(df):
        #     ewc = ['B04', 'F01', 'F02', 'F06', 'F07']
        #     return df[df['VerwerkingsmethodeCode'].isin(ewc)]
        # compute_lma_waste(lma_flows, role='Herkomst', apply=incineration_waste, text=text)
        #
        # # landfill waste
        # text = 'Landfill waste'
        # def landfill_waste(df):
        #     ewc = ['G01', 'G02']
        #     return df[df['VerwerkingsmethodeCode'].isin(ewc)]
        # compute_lma_waste(lma_flows, role='Herkomst', apply=landfill_waste, text=text)
        #
        # # reuse of primary waste
        # text = 'Reuse of primary waste'
        # def reuse_primary_waste(df):
        #     ewc = ['B01', 'B03', 'B05']
        #     return df[(df['EuralCode'].str[:2] != '19') & (df['VerwerkingsmethodeCode'].isin(ewc))]
        # compute_lma_waste(lma_flows, role='Herkomst', apply=reuse_primary_waste, text=text)
        #
        # # recycling of primary waste
        # text = 'Recycling of primary waste'
        # def recycling_primary_waste(df):
        #     ewc = ['C01', 'C02', 'C03', 'C04', 'D01',
        #            'D02', 'D03', 'D04', 'D05', 'D06',
        #            'E01', 'E02', 'E03',  'E04', 'E05',
        #            'F03', 'F04' ]
        #     return df[(df['EuralCode'].str[:2] != '19') & (df['VerwerkingsmethodeCode'].isin(ewc))]
        # compute_lma_waste(lma_flows, role='Herkomst', apply=recycling_primary_waste, text=text)

        # # total household waste per inhabitant
        # text = 'Total household waste per inhabitant'
        # print(text)
        # logging.info(text)
        # amounts = []
        # for idx, row in cbs_flows[cbs_flows['Provincie'] == 'Utrecht'].iterrows():
        #     name = row['Gebieden']
        #     amount = row["Totaal aangeboden huishoudelijk afval [Kilo's per inwoner]"]
        #     if not np.isnan(amount): amounts.append(amount)
        #     logging.info(f'{name}: {amount}')
        # logging.info(f'PROVINCIE MEAN: {sum(amounts) / len(amounts)}')
        # logging.info('\n')
        #
        # # household residual waste per inhabitant
        # text = 'Household residual waste per inhabitant (kg)'
        # print(text)
        # logging.info(text)
        # amounts = []
        # for idx, row in cbs_flows[cbs_flows['Provincie'] == 'Utrecht'].iterrows():
        #     name = row['Gebieden']
        #     amount = row["Hoeveelheid fijn huishoudelijk restafval [Kilo's per inwoner]"] +\
        #              row["Hoeveelheid grof huishoudelijk restafval [Kilo's per inwoner]"]
        #     if not np.isnan(amount): amounts.append(amount)
        #     logging.info(f'{name}: {amount}')
        # logging.info(f'PROVINCIE MEAN: {sum(amounts) / len(amounts)}')
        # logging.info('\n')
        #
        # # separation of household waste (as % of household waste)
        # text = 'Separation of household waste (as % of household waste)'
        # print(text)
        # logging.info(text)
        # amounts = []
        # for idx, row in cbs_flows[cbs_flows['Provincie'] == 'Utrecht'].iterrows():
        #     name = row['Gebieden']
        #     amount = row['Scheidingspercentage totaal huishoudelijk afval [Percentage]'] * 100
        #     if not np.isnan(amount): amounts.append(amount)
        #     logging.info(f'{name}: {amount}')
        # logging.info(f'PROVINCIE MEAN: {sum(amounts) / len(amounts)}')
        # logging.info('\n')

        # # residual waste of companies, organisations & governments
        # text = 'Residual waste of companies, organisations & governments'
        # def residual_waste(df):
        #     ewc = ['200301', '200307', '200399']
        #     return df[df['EuralCode'].isin(ewc)]
        # compute_lma_waste(lma_flows, role='Herkomst', apply=residual_waste, text=text)
        #
        # # reuse of construction & demolition waste
        # text = 'Reuse of construction & demolition waste'
        # def reuse_construction_waste(df):
        #     ewc = ['B01', 'B03', 'B05']
        #     return df[(df['EuralCode'].str[:2] == '17') & (df['VerwerkingsmethodeCode'].isin(ewc))]
        # compute_lma_waste(lma_flows, role='Herkomst', apply=reuse_construction_waste, text=text)
        #
        # # recycling of construction & demolition waste
        # text = 'Recycling of construction & demolition waste'
        # def recycling_construction_waste(df):
        #     ewc = ['B01', 'B03', 'B05']
        #     return df[(df['EuralCode'].str[:2] == '17') & (df['VerwerkingsmethodeCode'].isin(ewc))]
        # compute_lma_waste(lma_flows, role='Herkomst', apply=recycling_construction_waste, text=text)
        #
        # # food waste
        # text = 'Food waste'
        # def food_waste(df):
        #     ewc = ['020102', '020103', '020201', '020202', '020203',
        #            '020301', '020303', '020304', '020501', '020601',
        #            '020701', '020702', '020704', '200301', '200399',
        #            '200108', '200125', '200302']
        #     return df[df['EuralCode'].isin(ewc)]
        # compute_lma_waste(lma_flows, role='Herkomst', apply=food_waste, text=text)
        #
        # print('\n')
