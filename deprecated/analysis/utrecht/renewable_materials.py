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
        # split the materials column
        year_df['split_materials'] = year_df['materials'].str.split('&')

        # boolean lists for each row
        contains_biotisch = year_df['split_materials'].apply(
            lambda xs: [("Biotisch" in s) for s in xs]
        )
        contains_abiotisch = year_df['split_materials'].apply(
            lambda xs: [("Abiotisch" in s) for s in xs]
        )

        # groups
        year_df_none = year_df[contains_abiotisch.apply(all)]  # all Abiotisch
        year_df_all = year_df[contains_biotisch.apply(all)]  # all Biotisch

        # some = neither pure Abiotisch nor pure Biotisch
        year_df_some = year_df[
            ~(contains_abiotisch.apply(all)) &
            ~(contains_biotisch.apply(all))
        ]

        # compute stats
        total = {
            k: v for k, v in utils.get_classification_graphs(
                year_df,
                klass='agendas',
                unit=UNIT
            ).items() if k in ["agendas", "values"]
        }
        not_renew = {
            k: v for k, v in utils.get_classification_graphs(
                year_df_none,
                area=var.AREA,
                klass='agendas',
                unit=UNIT
            ).items() if k in ["agendas", "values"]
        }
        renew = {
            k: v for k, v in utils.get_classification_graphs(
                year_df_all,
                area=var.AREA,
                klass='agendas',
                unit=UNIT
            ).items() if k in ["agendas", "values"]
        }
        mixed = {
            k: v for k, v in utils.get_classification_graphs(
                year_df_some,
                area=var.AREA,
                klass='agendas',
                unit=UNIT
            ).items() if k in ["agendas", "values"]
        }

        DATA.setdefault('goods', []).append({
            'year': year,
            'unit': UNIT,
            'total': total,
            'not_renew': not_renew,
            'renew': renew,
            'mixed': mixed
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
        # split the materials column
        df['split_materials'] = df['materials'].str.split('&')

        # boolean lists for each row
        contains_biotisch = df['split_materials'].apply(
            lambda xs: [("Biotisch" in s) for s in xs]
        )
        contains_abiotisch = df['split_materials'].apply(
            lambda xs: [("Abiotisch" in s) for s in xs]
        )

        # groups
        df_none = df[contains_abiotisch.apply(all)]  # all Abiotisch
        df_all = df[contains_biotisch.apply(all)]  # all Biotisch

        # some = neither pure Abiotisch nor pure Biotisch
        df_some = df[
            ~(contains_abiotisch.apply(all)) &
            ~(contains_biotisch.apply(all))
            ]

        # compute stats
        total = {
            k: v for k, v in utils.get_classification_graphs(
                df,
                klass='agendas',
                unit=UNIT
            ).items() if k in ["agendas", "values"]
        }
        not_renew = {
            k: v for k, v in utils.get_classification_graphs(
                df_none,
                klass='agendas',
                unit=UNIT
            ).items() if k in ["agendas", "values"]
        }
        renew = {
            k: v for k, v in utils.get_classification_graphs(
                df_all,
                klass='agendas',
                unit=UNIT
            ).items() if k in ["agendas", "values"]
        }
        mixed = {
            k: v for k, v in utils.get_classification_graphs(
                df_some,
                klass='agendas',
                unit=UNIT
            ).items() if k in ["agendas", "values"]
        }

        DATA.setdefault('waste', []).append({
            'year': year,
            'unit': UNIT,
            'total': total,
            'not_renew': not_renew,
            'renew': renew,
            'mixed': mixed
        })


def run():
    process_cbs()
    process_waste()

    return DATA