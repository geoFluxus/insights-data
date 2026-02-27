import pandas as pd
import variables as var
from src.analysis import utils


DATA = {}
UNIT = 'kt'


def process_cbs(indicator=None):
    # DMI -> kt (million kg)
    filename = f'{var.OUTPUT_DIR}/all_data.xlsx'
    df = pd.read_excel(filename)
    df['Gewicht_KG'] = df[indicator] * 10 ** 6
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

        DATA.setdefault(indicator, []).append({
            'year': year,
            'unit': UNIT,
            'not_renew': not_renew,
            'renew': renew,
            'mixed': mixed
        })


def run():
    process_cbs(indicator='DMI')
    process_cbs(indicator='DMC')

    return DATA