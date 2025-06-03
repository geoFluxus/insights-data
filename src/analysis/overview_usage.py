import variables as var
import pandas as pd
from src.analysis import utils


DATA = {}

def run():
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

    # SANKEY
    stromen = [
        'Aanbod_eigen_regio',
        'Invoer_nationaal',
        'Invoer_internationaal',
    ]
    usages = [
        'Consumptie huishoudens',
        'Dienstverlening bedrijven',
        'Investeringen vaste activa',
        'Overheid',
        'Productie goederen',
        'Verandering voorraden'
    ]

    values = DATA.setdefault("values", {})
    for usage in usages:
        for stroom in stromen:
            usage_sum = df[
                (df['Stroom'] == stroom) &
                (df['Gebruiksgroep_naam'] == usage)
            ]['Gewicht_KG'].sum()

            usage_name = usage.replace('_', ' ')
            values.setdefault(usage_name, []).append(
                utils.kg_to_unit(usage_sum, unit=unit)
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
