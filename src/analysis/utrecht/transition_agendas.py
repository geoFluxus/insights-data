import variables as var
import pandas as pd
from src.analysis import utils


def run():
    # data prefix
    level = 'COROP'
    period = var.YEAR
    unit = var.UNITS['MATERIALS']['TRANSITION_AGENDAS']

    # DMI -> kt (million kg)
    filename = f'{var.OUTPUT_DIR}/all_province_data.xlsx'
    df = pd.read_excel(filename)
    df['Gewicht_KG'] = df['DMI'] * 10 ** 6
    df['Gewicht_KG'] = df['Gewicht_KG'].astype('int64')

    # filter by year & COROPS
    # exclude afval and total sums
    df = df[
        (df['Jaar'] == var.YEAR)
    ]

    # import cbs classifications
    cbs_classifs = {}
    for classif in ['agendas']:
        file_path = f"{var.INPUT_DIR}/Database_LockedFiles/DATA/ontology/cbs_{classif}.csv"
        cbs_classifs[classif] = pd.read_csv(file_path, low_memory=False, sep=';')

    # add classifications
    for name, classif in cbs_classifs.items():
        df = utils.add_classification(df, classif, name=name,
                                      left_on='cbs',
                                      right_on='cbs')

    # get agendas per province
    values = {}
    provinces = df['Regionaam'].drop_duplicates().to_list()
    for province in provinces:
        province_df = df[df['Regionaam'] == province]
        values[province] = {
            k: v for k, v in utils.get_classification_graphs(
                province_df,
                area=province,
                klass='agendas',
                unit=unit
            ).items() if k in ["agendas", "values"]
        }

    # turn to percentages
    percentages = {}
    for province in provinces:
        agendas = values[province]['agendas']
        vals = values[province]['values']
        total_sum = sum(vals)
        percentages[province] = {
            'agendas': agendas,
            'values': [v / total_sum * 100 for v in vals]
        }

    return {
        'level': level,
        'period': period,
        'values': percentages,
        'area': var.COROPS[0],
        'unit': '%'
    }