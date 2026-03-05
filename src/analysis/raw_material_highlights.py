import pandas as pd
import variables as var

DATA = {}


def renewable_sum(df, indicator='DMI'):
    return (
        df.loc[df["renewable"].isin(["hernieuwbaar", "secundair"]), indicator].sum()
        + 0.5 * df.loc[df["renewable"] == "gemengd", indicator].sum()
    )


def run():
    # compute non-fossil indicators
    path = fr"{var.OUTPUT_DIR}/dmi_dmc.xlsx"
    for indicator in ['dmi', 'rmi', 'dmc', 'rmc']:
        df = pd.read_excel(path, sheet_name=indicator)
        DATA[indicator] = df[df['Jaar'] == var.YEAR][indicator.upper()].sum()

    DATA['dmi_dmc'] = DATA['dmi'] / DATA['dmc']

    # import fossil data
    path = fr"{var.OUTPUT_DIR}/all_data.xlsx"
    df = pd.read_excel(path, sheet_name='FE')
    DATA['fe'] = df[df['Jaar'] == var.YEAR]['DMI'].sum()
    DATA['dmi_fe'] = DATA['dmi'] + DATA['fe']

    # import not-fossil data
    path = fr"{var.OUTPUT_DIR}/all_data.xlsx"
    df = pd.read_excel(path, sheet_name='NON_FE')
    df = df[df['Jaar'] == var.YEAR]
    assert df['DMI'].sum() == DATA['dmi']

    # import renewable
    path = fr"{var.INPUT_DIR}\Database_LockedFiles\DATA\ontology\npce_hernieuwbaar.xlsx"
    renewable = pd.read_excel(path)
    df = pd.merge(df, renewable, on='cbs')

    # substitution
    DATA['dmc_sub'] = renewable_sum(df, indicator='DMC')

    return DATA
