import pandas as pd
import variables as var
from src.analysis import utils


DATA = {}
PRODUCTS = [key for key in var.PRODUCTGROEPEN]
RENEWABLES = [
    'hernieuwbaar',
    'secundair',
    'niet-hernieuwbaar',
    'gemengd',
    'fe'
]
UNIT = 'kt'


def process_cbs(indicator=None):
    concats = []
    for sheet in ['NON_FE', 'FE']:
        # import non-fossil
        filename = f'{var.OUTPUT_DIR}/all_data.xlsx'
        df = pd.read_excel(filename, sheet_name=sheet)

        # merge with renewable
        # mark all fossil groups as fossil (for split goederen)
        path = fr"{var.INPUT_DIR}\Database_LockedFiles\DATA\ontology\npce_hernieuwbaar.xlsx"
        renewable = pd.read_excel(path)
        df = pd.merge(df, renewable, on='cbs')
        if sheet == 'FE':
            df['renewable'] = 'fe'

        # merge with product groups
        path = fr"{var.INPUT_DIR}\Database_LockedFiles\DATA\ontology\npce_productgroepen.xlsx"
        productgroups = pd.read_excel(path, sheet_name='goederen')
        df = pd.merge(df, productgroups, on='cbs')

        concats.append(df)
    df = pd.concat(concats)

    data = {
        'products': {},
        'renew': RENEWABLES,
        'unit': UNIT
    }
    for product in PRODUCTS:
        for year in var.DMI_YEARS:
            cats, year_df = utils.split_categories(
                df[df['Jaar'] == year],
                column='productgroepen',
                extra=['renewable'],
                amount=indicator
            )

            for renewable in RENEWABLES:
                value = year_df[
                    (year_df['productgroepen'] == product) &\
                    (year_df['renewable'] == renewable)
                ][indicator].sum()
                prod_item = data['products'].setdefault(product, {})
                year_item = prod_item.setdefault(year, []).append(value)

    DATA[indicator.lower()] = data


def run():
    process_cbs(indicator='DMI')
    process_cbs(indicator='DMC')

    return DATA