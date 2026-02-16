import variables as var
import pandas as pd
from src.analysis import utils


DATA = {}


def run(on_agendas=False):
    unit = var.UNITS['OVERVIEW']['OVERVIEW_USAGE']

    # stromen -> million kg
    path = f"{var.INPUT_DIR}/Database_LockedFiles/DATA/monitor_data/data/CBS"
    filename = f"{path}/{var.COROP_FILE}.csv"

    df = pd.read_csv(filename, low_memory=False, sep=',')
    df['Gewicht_KG'] = df['Brutogew'] * 10 ** 6  # mln kg -> kg
    df['Gewicht_KG'] = df['Gewicht_KG'].astype('int64')

    # filter by year & COROPS
    # exclude afval and total sums
    df = df[
        (df['Jaar'] == var.YEAR) &
        (df['Regionaam'].isin(var.COROPS)) &
        (~df['Goederengroep_naam'].str.contains('afval', case=False, na=False)) &
        (df['Gebruiksgroep_naam'] != 'Totaal')
    ]

    # import cbs classifications
    cbs_classifs = {}
    for classif in ['agendas']:
        file_path = f"{var.INPUT_DIR}/Database_LockedFiles/DATA/ontology/cbs_{classif}.csv"
        cbs_classifs[classif] = pd.read_csv(file_path, low_memory=False, sep=';')

    # add classifications
    for name, classif in cbs_classifs.items():
        df = utils.add_classification(df, classif, name=name,
                                      left_on='Goederengroep_nr',
                                      right_on='cbs')

    # SANKEY
    stromen = [
        'Aanbod_eigen_regio',
        'Invoer_nationaal',
        'Invoer_internationaal',
    ]
    usages = [
        'Consumptie huishoudens',
        'Dienstverlening bedrijven',
        'Productie goederen',
        'Overheid',
        'Investeringen vaste activa',
        'Verandering voorraden'
    ]

    values = DATA.setdefault("values", {})
    for usage in usages:
        for stroom in stromen:
            usage_df = df[
                (df['Stroom'] == stroom) &
                (df['Gebruiksgroep_naam'] == usage)
            ]
            usage_name = usage.replace('_', ' ')

            if on_agendas:
                values.setdefault(usage_name, []).append({
                    k: v for k, v in utils.get_classification_graphs(
                        usage_df,
                        area=var.COROPS,
                        klass='agendas',
                        unit=unit
                    ).items() if k in ["agendas", "values"]
                })
            else:
                values.setdefault(usage_name, []).append(
                    utils.kg_to_unit(usage_df['Gewicht_KG'].sum(), unit=unit)
                )

    return {
        "level": "COROP",
        "name": var.COROPS[0],
        "period": var.YEAR,
        "type": "goederen",
        "unit": unit,
        "usage": [stroom.replace('_', ' ') for stroom in stromen],
        **DATA
    }
