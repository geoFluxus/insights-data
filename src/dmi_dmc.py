import pandas as pd
import numpy as np
import variables as var


FE_GROUPS = [
    'Steenkool en bruinkool',
    'Ruwe aardolie',
    'Aardgas',
]

SPLIT_FE = [
    'Cokes en vaste aardolieproducten',
    'Vloeibare aardolieproducten',
    'Gasvormige aardolieproducten',
    'Chemische basisproducten',
    'Overig afval en secundaire grondstoffen, biomassa',
    'Overig afval en secundaire grondstoffen, fossiel'
]

GEBRUIK = [
    'Consumptie huishoudens',
    'Dienstverlening bedrijven',
    'Investeringen vaste activa',
    'Niet van toepassing',
    'Overheid',
    'Productie goederen',
    'Verandering voorraden'
]

IS_FE = {
    'Cokes en vaste aardolieproducten': {
        'Productie goederen': 50,
        'Dienstverlening bedrijven': 100,
        'Overheid': 100,
        'Consumptie huishoudens': 100,
    },
    'Vloeibare aardolieproducten': {
        'Productie goederen': 50,
        'Dienstverlening bedrijven': 100,
        'Overheid': 100,
        'Consumptie huishoudens': 100,
    },
    'Gasvormige aardolieproducten': {
        'Productie goederen': 50,
        'Dienstverlening bedrijven': 100,
        'Overheid': 100,
        'Consumptie huishoudens': 100,
    },
    'Chemische basisproducten': {
        'Productie goederen': 50,
        'Dienstverlening bedrijven': 100,
        'Overheid': 100,
        'Consumptie huishoudens': 100,
    },
    'Overig afval en secundaire grondstoffen, biomassa': {
        'Productie goederen': 50,
        'Dienstverlening bedrijven': 50,
    },
    'Overig afval en secundaire grondstoffen, fossiel': {
        'Productie goederen': 50,
    },
}


DATA = {}
RELEVANT_COLS = [
    'Regionaam',
    'Stroom',
    'Goederengroep_naam',
    'Waarde',
    'Brutogew'
]

PROJ_START = 2015
PROJ_END = 2030

RESOURCE_TYPE = None
FILEPATH = None


def split_fossil(df, is_fossil=False):
    """
    For SPLIT_FE goods, apply fossil fraction perc to fossil run, and (100-perc) to non-fossil run.
    If usage not in IS_FE[good], default perc=0 => 0% fossil, 100% non-fossil.
    """
    for good in SPLIT_FE:
        for usage in GEBRUIK:
            condition = (
                (df['Goederengroep_naam'] == good) &
                (df['Gebruiksgroep_naam'] == usage)
            )
            perc = IS_FE[good].get(usage, 0)
            factor = (perc if is_fossil else 100 - perc) / 100
            for value in ['Brutogew', 'Waarde']:
                df.loc[condition, value] = df.loc[condition, value] * factor
    return df


def compute_local_extraction(data, value=None, lokale_winning_groups=None):
    data = pd.pivot_table(
        data,
        values=value,
        columns="Stroom",
        index=["Regionaam", "Goederengroep_naam", "Gebruiksgroep_naam"],
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    data.columns = [
        c[1] if isinstance(c, tuple) and c[0] == value else (c[0] if isinstance(c, tuple) else c)
        for c in data.columns.to_flat_index()
    ]
    data = data.rename(columns={"Goederengroep_naam": "Goederengroep"})

    for col in ["Uitvoer_nationaal", "Uitvoer_internationaal", "Aanbod_eigen_regio"]:
        if col not in data.columns:
            data[col] = 0

    # goederen-level winning table (unique per Regionaam+Goederengroep)
    lw = data[data["Goederengroep"].isin(lokale_winning_groups)].copy()
    lw["Winning"] = (
        lw["Uitvoer_nationaal"] + lw["Uitvoer_internationaal"] + lw["Aanbod_eigen_regio"]
    )
    lw = lw.groupby(["Regionaam", "Goederengroep"], as_index=False)["Winning"].sum()

    data = data.merge(lw, how="left", on=["Regionaam", "Goederengroep"], validate="m:1")
    data["Winning"] = data["Winning"].fillna(0)

    # ✅ write Winning only once per good (prevents double counting across gebruiksgroep rows)
    first_row = data.groupby(["Regionaam", "Goederengroep"]).cumcount().eq(0)
    data["Winning"] = np.where(first_row, data["Winning"], 0)

    # resource types
    data = data.merge(
        RESOURCE_TYPE.drop_duplicates(["Goederengroep"]),
        on="Goederengroep",
        how="left",
        validate="m:1"
    )

    return data


def calculate_rmi_rmc(df, eur_df, year, save=False, abiotisch=False):
    cols_import = ['Winning', 'Invoer_nationaal', 'Invoer_internationaal']
    cols_export = ['Uitvoer_nationaal', 'Uitvoer_internationaal']
    rme_matrices_file = 'geoFluxus/CBS_to_RME.xlsx'

    cbs_rme = pd.read_excel(f'{FILEPATH}/{rme_matrices_file}', sheet_name='CBS_to_RME_codes').fillna(0)
    eur_or_t = pd.read_excel(f'{FILEPATH}/{rme_matrices_file}', sheet_name='eur_or_t')
    eur_or_t.set_index(eur_or_t['CBS_name'])

    rme_import_coefficients = pd.read_excel(f'{FILEPATH}/{rme_matrices_file}', sheet_name='RME_import_' + str(year))
    rme_export_coefficients = pd.read_excel(f'{FILEPATH}/{rme_matrices_file}', sheet_name='RME_export_' + str(year))
    rm_groups_import = rme_import_coefficients['Raw_material_name'][1:]
    rm_groups_export = rme_export_coefficients['Raw_material_name'][1:]

    convert_import = cbs_rme.values[1:, 1:].astype(float) @ rme_import_coefficients.values[1:, 2:].T
    converter_import = pd.DataFrame(index=cbs_rme['CBS_name'][1:], columns=rm_groups_import, data=convert_import)

    convert_export = cbs_rme.values[1:, 1:].astype(float) @ rme_export_coefficients.values[1:, 2:].T
    converter_export = pd.DataFrame(index=cbs_rme['CBS_name'][1:], columns=rm_groups_export, data=convert_export)

    if save:
        converter_import.to_excel(FILEPATH + f'cbs_to_rme_conversion_table_import_{year}.xlsx')
        converter_export.to_excel(FILEPATH + f'cbs_to_rme_conversion_table_export_{year}.xlsx')

    rm_data = pd.DataFrame()

    df = pd.merge(df, eur_or_t, left_on='Goederengroep', right_on='CBS_name', how='left')
    eur_df = pd.merge(eur_df, eur_or_t, left_on='Goederengroep', right_on='CBS_name', how='left')

    rm_data[['Jaar', 'Regionaam', 'Goederengroep']] = df[['Jaar', 'Regionaam', 'Goederengroep']]
    rm_data.set_index(['Jaar', 'Regionaam', 'Goederengroep'], inplace=True)
    df.set_index(['Jaar', 'Regionaam', 'Goederengroep'], inplace=True)
    eur_df.set_index(['Jaar', 'Regionaam', 'Goederengroep'], inplace=True)

    for col in cols_import:
        rm_data[col] = eur_df['eur'] * eur_df[col] + df['ton'] * df[col]
    for col in cols_export:
        rm_data[col] = eur_df['eur'] * eur_df[col] + df['ton'] * df[col]

    rm_data.reset_index(inplace=True)
    df.reset_index(inplace=True)
    eur_df.reset_index(inplace=True)

    df_import = pd.merge(rm_data, converter_import, left_on='Goederengroep', right_index=True, how='left').fillna(0)

    out_cols = []
    for i in rm_groups_import:
        for j in cols_import:
            out_cols.append((i, j))
    df_import = pd.concat([df_import, pd.DataFrame(0, columns=out_cols, index=df_import.index)], axis=1)

    for i in rm_groups_import:
        for j in cols_import:
            df_import[i, j] += df_import[i] * df_import[j]

    materials = df_import.groupby(['Regionaam', 'Jaar'])[out_cols].sum()
    materials.columns = pd.MultiIndex.from_tuples(out_cols)
    materials = materials.stack(level=0, future_stack=True)

    if abiotisch:
        abiotics = pd.read_excel(f'{FILEPATH}/{rme_matrices_file}', sheet_name='abiotisch')
        materials = materials[materials.index.get_level_values(2).isin(abiotics['Abiotisch'])]

    materials['RMI'] = materials['Winning'] + materials['Invoer_internationaal'] + materials['Invoer_nationaal']

    df_export = pd.merge(rm_data, converter_export, left_on='Goederengroep', right_index=True, how='left').fillna(0)

    out_cols_exp = []
    for i in rm_groups_export:
        for j in cols_export:
            out_cols_exp.append((i, j))
    df_export = pd.concat([df_export, pd.DataFrame(0, columns=out_cols_exp, index=df_export.index)], axis=1)

    for i in rm_groups_export:
        for j in cols_export:
            df_export[i, j] += df_export[i] * df_export[j]

    materials_export = df_export.groupby(['Regionaam', 'Jaar'])[out_cols_exp].sum()
    materials_export.columns = pd.MultiIndex.from_tuples(out_cols_exp)
    materials_export = materials_export.stack(level=0, future_stack=True)

    if abiotisch:
        materials_export = materials_export[materials_export.index.get_level_values(2).isin(abiotics['Abiotisch'])]

    materials = pd.merge(materials, materials_export, left_index=True, right_index=True, how='outer')
    materials['RMC'] = materials['RMI'] - materials['Uitvoer_nationaal'] - materials['Uitvoer_internationaal']

    return materials.reset_index()


def calculate_indicators(path, file_name, corop=var.COROPS, raw_materials=False, goal='abiotisch', is_fossil=False):
    dmcs = pd.DataFrame()
    dmis = pd.DataFrame()
    all_data = pd.DataFrame()
    all_eur_data = pd.DataFrame()
    all_rm_data = pd.DataFrame()

    df = pd.read_csv(path + file_name, low_memory=False, sep=',')
    df = df.dropna(how='all', axis='columns')

    for year in var.DMI_YEARS:
        if isinstance(corop, str):
            corop = [corop]

        df_year = df[
            (df['Jaar'] == year) &
            (df['Regionaam'].isin(corop)) &
            # (df['Goederengroep_naam'] != 'Huishoudelijk afval en gemeentelijk afval') &
            (~df['Goederengroep_naam'].str.contains('afval', case=False, na=False)) &
            (df['Gebruiksgroep_naam'] != 'Totaal')
        ].copy()

        fossil_groups = df_year['Goederengroep_naam'].isin(FE_GROUPS)
        if not is_fossil:
            fossil_groups = ~(fossil_groups)
        else:
            split_groups = df_year['Goederengroep_naam'].isin(SPLIT_FE)
            fossil_groups = fossil_groups | split_groups
        df_year = df_year[fossil_groups]

        df_year = split_fossil(df_year, is_fossil=is_fossil)

        lokale_winning_groups = RESOURCE_TYPE[RESOURCE_TYPE['Lokale winning'] == 'ja']['Goederengroep'].tolist()

        data = compute_local_extraction(df_year, value="Brutogew", lokale_winning_groups=lokale_winning_groups)

        if raw_materials:
            eur_data = compute_local_extraction(df_year, value="Waarde", lokale_winning_groups=lokale_winning_groups)

        if 'abiotisch' in goal:
            abiotisch = data[data['Grondstof'] == 'abiotisch']

            abiotisch_in_gemengd = data[data['Grondstof'] == 'gemengd']
            abiotisch_in_gemengd = abiotisch_in_gemengd.apply(lambda x: x * 0.5 if x.dtype == 'float64' else x)

            all_abiotisch = pd.concat([abiotisch, abiotisch_in_gemengd])

            aggregated = all_abiotisch.groupby(['Regionaam']).sum(numeric_only=True).reset_index()
            if raw_materials:
                rm_data = data.copy()
                eur_aggregated = eur_data.copy()

        elif goal == 'agg_per_type':
            aggregated = data.groupby(['Regionaam', 'Grondstof']).sum(numeric_only=True).reset_index()
            if raw_materials:
                eur_aggregated = eur_data.groupby(['Regionaam', 'Grondstof']).sum(numeric_only=True).reset_index()

        elif goal == 'agg_per_province':
            aggregated = data.groupby(['Regionaam']).sum(numeric_only=True).reset_index()
            if raw_materials:
                eur_aggregated = eur_data.groupby(['Regionaam']).sum(numeric_only=True).reset_index()

        else:
            aggregated = data.copy()
            if raw_materials:
                eur_aggregated = eur_data.copy()

        aggregated['DMI'] = aggregated['Winning'] + aggregated['Invoer_nationaal'] + aggregated['Invoer_internationaal']
        aggregated['DMC'] = aggregated['DMI'] - aggregated['Uitvoer_nationaal'] - aggregated['Uitvoer_internationaal']
        aggregated['National_DMI'] = aggregated['Winning'] + aggregated['Invoer_internationaal']
        aggregated['Jaar'] = year

        if raw_materials:
            eur_aggregated['Jaar'] = year

        if not 'abiotisch' in goal:
            outcomes_rm = calculate_rmi_rmc(aggregated, eur_aggregated, year, save=True)
        else:
            rm_data['Jaar'] = year
            outcomes_rm = calculate_rmi_rmc(rm_data, eur_aggregated, year, save=True, abiotisch=True)

        dmc = aggregated[['Regionaam', 'DMC', 'Jaar']].copy(deep=True)
        dmi = aggregated[['Regionaam', 'DMI', 'Jaar']].copy(deep=True)

        dmcs = pd.concat([dmcs, dmc], ignore_index=True)
        dmis = pd.concat([dmis, dmi], ignore_index=True)

        if raw_materials:
            all_rm_data = pd.concat([all_rm_data, outcomes_rm], ignore_index=True)
            all_eur_data = pd.concat([all_eur_data, eur_aggregated], ignore_index=True)

        all_data = pd.concat([all_data, aggregated], ignore_index=True)

    if raw_materials:
        rmcs = all_rm_data[['Regionaam', 'RMC', 'Jaar']].copy()
        rmis = all_rm_data[['Regionaam', 'RMI', 'Jaar']].copy()
        return dmcs, dmis, rmcs, rmis, all_data, all_eur_data, all_rm_data
    else:
        return dmcs, dmis


def _aggregate_no_gebruik(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build ALL sheet:
    - combine FE + NON_FE
    - drop Gebruiksgroep_naam
    - KEEP Jaar as a grouping key (do NOT sum it)
    """
    if df is None or df.empty:
        return df

    df = df.copy()
    df = df.drop(columns=['Gebruiksgroep_naam'], errors='ignore')

    # Jaar must be a key, not a numeric column to sum
    force_key_cols = {'Jaar'}

    num_cols = [
        c for c in df.columns
        if pd.api.types.is_numeric_dtype(df[c]) and c not in force_key_cols
    ]
    key_cols = [c for c in df.columns if c not in num_cols]

    # ensure Jaar is in keys (even if it was numeric)
    for c in force_key_cols:
        if c in df.columns and c not in key_cols:
            key_cols.append(c)

    return df.groupby(key_cols, as_index=False)[num_cols].sum()


def _write_three_sheets(excel_path: str, non_fe: pd.DataFrame, fe: pd.DataFrame):
    """
    Write NON_FE, FE, and ALL.
    ALL = NON_FE + FE, then aggregate away Gebruiksgroep_naam.
    """
    all_df = pd.concat([non_fe, fe], ignore_index=True)
    all_df = _aggregate_no_gebruik(all_df)

    with pd.ExcelWriter(excel_path, engine="openpyxl", mode="w") as writer:
        non_fe.to_excel(writer, sheet_name="NON_FE", index=False)
        fe.to_excel(writer, sheet_name="FE", index=False)
        all_df.to_excel(writer, sheet_name="ALL", index=False)


def run():
    global FILEPATH, RESOURCE_TYPE

    print("\nDMI-RMI")
    FILEPATH = f"{var.INPUT_DIR}/Database_LockedFiles/DATA/monitor_data/data"

    filename = f"/CBS/{var.COROP_FILE}.csv"

    path = f"{FILEPATH}/geofluxus"
    RESOURCE_TYPE = pd.read_csv(f'{path}/cbs_biotisch_abiotisch_2024_final.csv', delimiter=';')

    # --- NON_FE ---
    dmcs_non, dmis_non, rmcs_non, rmis_non, all_data_non, all_eur_non, all_rm_non = calculate_indicators(
        FILEPATH, filename,
        raw_materials=True,
        goal='total',
        is_fossil=False
    )

    # --- FE ---
    dmcs_fe, dmis_fe, rmcs_fe, rmis_fe, all_data_fe, all_eur_fe, all_rm_fe = calculate_indicators(
        FILEPATH, filename,
        raw_materials=True,
        goal='total',
        is_fossil=True
    )

    # Export 3 workbooks with NON_FE / FE / ALL
    _write_three_sheets(f"{var.OUTPUT_DIR}/all_raw_material_data.xlsx", all_rm_non, all_rm_fe)
    _write_three_sheets(f"{var.OUTPUT_DIR}/all_data.xlsx", all_data_non, all_data_fe)
    _write_three_sheets(f"{var.OUTPUT_DIR}/euro_data_all.xlsx", all_eur_non, all_eur_fe)

    # Keep your original “dmi_dmc.xlsx” export (NON_FE only by default)
    dmcs_ab_non, dmis_ab_non, rmcs_ab_non, rmis_ab_non, _, _, _ = calculate_indicators(
        FILEPATH, filename,
        raw_materials=True,
        is_fossil=False
    )

    sheets = {
        'dmc': dmcs_non,
        'dmi': dmis_non,
        'rmc': rmcs_non,
        'rmi': rmis_non,
        'dmc_ab': dmcs_ab_non,
        'dmi_ab': dmis_ab_non,
        'rmc_ab': rmcs_ab_non,
        'rmi_ab': rmis_ab_non,
    }

    with pd.ExcelWriter(f"{var.OUTPUT_DIR}/dmi_dmc.xlsx", engine="openpyxl") as writer:
        for indicator, data in sheets.items():
            data.to_excel(writer, sheet_name=indicator[:31], index=False)


if __name__ == "__main__":
    run()