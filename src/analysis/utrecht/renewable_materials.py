import pandas as pd
import variables as var
from src.analysis import utils


DATA = {}


def process_cbs():
    # DMI -> kt (million kg)
    filename = f'{var.OUTPUT_DIR}/all_data.xlsx'
    df = pd.read_excel(filename)
    df['Gewicht_KG'] = df['DMI'] * 10 ** 6
    df['Gewicht_KG'] = df['Gewicht_KG'].astype('int64')

    # filter by year & COROPS
    # exclude afval and total sums
    df = df[df['Regionaam'].isin(var.COROPS)]

    # import cbs classifications
    cbs_classifs = {}
    for classif in ['materials']:
        file_path = f"{var.INPUT_DIR}/Database_LockedFiles/DATA/ontology/cbs_{classif}.csv"
        cbs_classifs[classif] = pd.read_csv(file_path, low_memory=False, sep=';')

    # add classifications
    for name, classif in cbs_classifs.items():
        df = utils.add_classification(df, classif, name=name,
                                      left_on='cbs',
                                      right_on='cbs')

    for year in var.GOALS_YEARS:
        year_df = df[df['Jaar'] == year]
        total_sum = year_df['Gewicht_KG'].sum()
        renew_sum = year_df[
            year_df['materials'].str.contains('Biotisch')
        ]['Gewicht_KG'].sum()
        DATA.setdefault('goods', []).append({
            'year': year,
            'total_sum': total_sum / 10**9,
            'renew_sum': renew_sum / 10**9,
            'unit': 'Mt'
        })


def process_waste():
    # add areas to roles
    print('Add areas to roles...')
    POLYGON = utils.import_areas(level=var.LEVEL)
    POLYGON = POLYGON[POLYGON['name'] == var.AREA]
    source = var.ROLES['Ontvangst']['source']  # source role

    # import cbs classifications
    ewc_classifs = {}
    for classif in ['materials']:
        file_path = f"{var.INPUT_DIR}/Database_LockedFiles/DATA/ontology/ewc_{classif}.csv"
        ewc_classifs[classif] = pd.read_csv(file_path, low_memory=False, sep=';')

    # process lma files
    path = f"{var.INPUT_DIR}/{var.AREA_DIR}/LMA/processed"
    for year in var.GOALS_YEARS:
        # import year file
        filename = f"{path}/ontvangst_{var.AREA.lower()}_{year}_full.csv"
        df = pd.read_csv(filename, low_memory=False)
        df = utils.add_areas(df,
                             areas=POLYGON,
                             role=source,
                             admin_level=var.LEVEL)
        df = df[df[f'Herkomst_{var.LEVEL}'] == var.AREA]

        # add classifications
        for name, classif in ewc_classifs.items():
            df = utils.add_classification(df, classif, name=name,
                                          left_on='EuralCode',
                                          right_on='ewc')
        df['split_materials'] = df['materials'].str.split('&')

        # compute stats
        total_sum = df['Gewicht_KG'].sum()
        renew_sum = df[
            df['split_materials'].apply(lambda x: all('Biotisch' in s for s in x))
        ]['Gewicht_KG'].sum()
        DATA.setdefault('waste', []).append({
            'year': year,
            'total_sum': total_sum / 10 ** 9,
            'renew_sum': renew_sum / 10 ** 9,
            'unit': 'Mt'
        })


def run():
    process_cbs()
    process_waste()

    return DATA