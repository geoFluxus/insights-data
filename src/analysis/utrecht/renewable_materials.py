import pandas as pd
import variables as var
from src.analysis import utils


DATA = {}
UNIT = 'kt'


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
    for classif in ['agendas', 'materials']:
        file_path = f"{var.INPUT_DIR}/Database_LockedFiles/DATA/ontology/cbs_{classif}.csv"
        cbs_classifs[classif] = pd.read_csv(file_path, low_memory=False, sep=';')

    # add classifications
    for name, classif in cbs_classifs.items():
        df = utils.add_classification(df, classif, name=name,
                                      left_on='cbs',
                                      right_on='cbs')

    for year in var.GOALS_YEARS:
        year_df = df[df['Jaar'] == year]
        total = {
            k: v for k, v in utils.get_classification_graphs(
                year_df,
                klass='agendas',
                unit=UNIT
            ).items() if k in ["agendas", "values"]
        }
        renew = {
            k: v for k, v in utils.get_classification_graphs(
                year_df[year_df['materials'].str.contains('Biotisch')],
                area=var.AREA,
                klass='agendas',
                unit=UNIT
            ).items() if k in ["agendas", "values"]
        }
        DATA.setdefault('goods', []).append({
            'year': year,
            'unit': UNIT,
            'total': total,
            'renew': renew
        })


def process_waste():
    # add areas to roles
    print('Add areas to roles...')
    POLYGON = utils.import_areas(level=var.LEVEL)
    POLYGON = POLYGON[POLYGON['name'] == var.AREA]
    source = var.ROLES['Ontvangst']['source']  # source role

    # import cbs classifications
    ewc_classifs = {}
    for classif in ['agendas', 'materials']:
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
        total = {
            k: v for k, v in utils.get_classification_graphs(
                df,
                source=source,
                level=var.LEVEL,
                area=var.AREA,
                klass='agendas',
                unit=UNIT
            ).items() if k in ["agendas", "values"]
        }
        renew = {
            k: v for k, v in utils.get_classification_graphs(
                df[df['split_materials'].apply(lambda x: all('Biotisch' in s for s in x))],
                source=source,
                level=var.LEVEL,
                area=var.AREA,
                klass='agendas',
                unit=UNIT
            ).items() if k in ["agendas", "values"]
        }
        DATA.setdefault('waste', []).append({
            'year': year,
            'unit': UNIT,
            'total': total,
            'renew': renew
        })


def run():
    process_cbs()
    process_waste()

    return DATA