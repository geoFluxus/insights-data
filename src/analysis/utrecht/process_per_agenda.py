import pandas as pd
import variables as var
from src.analysis import utils


def get_flows(year=None):
    """
    Import flows from 'data/flows'
    """
    path = f"{var.INPUT_DIR}/{var.AREA_DIR}/LMA/processed"
    filename = f"{path}/ontvangst_{var.AREA.lower()}_{year}_full.csv"

    return pd.read_csv(filename, low_memory=False)


def run():
    unit = 'kt'

    # import areas
    # import province polygon
    polygon = utils.import_areas(level=var.LEVEL)
    polygon = polygon[polygon['name'] == var.AREA]
    assert len(polygon) == 1

    # import data
    source = 'Herkomst'
    df = get_flows(year=var.YEAR)
    df = utils.add_areas(df, role=source, areas=polygon, admin_level=var.LEVEL)
    df = df[df[f'{source}_{var.LEVEL}'] == var.AREA]

    # import ewc classifications
    ewc_classifs = {}
    for classif in ['agendas']:
        file_path = f"{var.INPUT_DIR}/Database_LockedFiles/DATA/ontology/ewc_{classif}.csv"
        ewc_classifs[classif] = pd.read_csv(file_path, low_memory=False, sep=';')

    # add classifications
    for name, classif in ewc_classifs.items():
        df = utils.add_classification(df, classif, name=name,
                                      left_on='EuralCode',
                                      right_on='ewc')

    # import rladder
    print("Import rladder...")
    path = f"{var.INPUT_DIR}/Database_LockedFiles/DATA/descriptions/rhierarchy.xlsx"
    rladder = pd.read_excel(path)
    rladder = rladder[['processing_code', 'benchmark_group']]
    rladder['benchmark_group'] = rladder['benchmark_group'].str[0]
    df = pd.merge(df, rladder,
                  how='left',
                  left_on=['VerwerkingsmethodeCode'],
                  right_on=['processing_code'])

    # agendas per process
    values = {}
    for code in rladder['benchmark_group'].to_list():
        code_df = df[df['benchmark_group'] == code]
        values[code] = {
            k: v for k, v in utils.get_classification_graphs(
                code_df,
                source=source,
                level=var.LEVEL,
                area=var.AREA,
                klass='agendas',
                unit=unit
            ).items() if k in ["agendas", "values"]
        }

    return {
        'level': var.LEVEL,
        'period': var.YEAR,
        'values': values,
        'area': var.COROPS[0],
        'unit': unit
    }