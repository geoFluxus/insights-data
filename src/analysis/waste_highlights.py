import pandas as pd
import variables as var
from src.analysis import utils

DATA = {}


def filter_by_area(df):
    ROLES = var.ROLES

    # import areas
    # import province polygon
    polygon = utils.import_areas(level=var.LEVEL)
    polygon = polygon[polygon['name'] == var.AREA]
    assert len(polygon) == 1

    # add areas to roles
    source = ROLES['Ontvangst']['source']  # source role
    df = utils.add_areas(df, role=source, areas=polygon, admin_level=var.LEVEL)

    # ONLY PRODUCTION
    return df[df[f"{source}_{var.LEVEL}"] == var.AREA]


def run():
    # import waste data
    print(f"\nImport province data for {var.YEAR}...")
    path = f"{var.INPUT_DIR}/{var.AREA_DIR}/LMA/processed"
    filename = f"{path}/ontvangst_{var.AREA.lower()}_{var.YEAR}_full.csv"
    df = pd.read_csv(filename, low_memory=False)
    df['EuralCode'] = df['EuralCode'].astype(str).str.zfill(6)
    print(f"\nFilter on production only within area...")
    df = filter_by_area(df)

    # import eural names
    path = f"{var.INPUT_DIR}/Database_LockedFiles/DATA/geofluxusApp/templates"
    ewc6 = pd.read_excel(f"{path}/waste06.xlsx")
    ewc6['ewc_code'] = ewc6['ewc_code'].astype(str).str.zfill(6)
    df = pd.merge(df, ewc6,
                  left_on='EuralCode',
                  right_on='ewc_code')

    # import process value
    path = fr"{var.INPUT_DIR}\Database_LockedFiles\DATA\ontology\npce_hoogwaardig.xlsx"
    process = pd.read_excel(path)
    df = pd.merge(df, process,
                  left_on='VerwerkingsmethodeCode',
                  right_on='LMA verwerkingscode')

    # recycle rates
    total_sum = df['Gewicht_KG'].sum()
    high_sum = df[
        df['Berekening NPCE doelstellingen'] == 'Hoogwaardige recycling'
    ]['Gewicht_KG'].sum()
    recycle_sum = df[
        df['Berekening NPCE doelstellingen'] == 'Recycling'
    ]['Gewicht_KG'].sum()
    DATA['high'] = {
        'value': high_sum / total_sum * 100 if total_sum != 0 else 0,
        'unit': '%'
    }
    DATA['recycle'] = {
        'value': recycle_sum / total_sum * 100 if total_sum != 0 else 0,
        'unit': '%'
    }
    DATA['total'] = {
        'value': total_sum / 10 ** 6,
        'unit': 'kt'
    }

    # highest eural
    sum_df = df.groupby(by=['EuralCode', 'ewc_name'], as_index=False)['Gewicht_KG'].sum()
    row = sum_df.loc[sum_df['Gewicht_KG'].idxmax()]
    DATA['highest'] = {
        'name': row['ewc_name']
    }

    return DATA
