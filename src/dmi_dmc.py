import pandas as pd
import variables as var
import seaborn as sns
from seaborn.regression import _RegressionPlotter


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


import numpy as np
import pandas as pd

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
    data = data.merge(RESOURCE_TYPE.drop_duplicates(["Goederengroep"]),
                      on="Goederengroep", how="left", validate="m:1")

    return data


def calculate_rmi_rmc(df, eur_df, year, save=False, abiotisch = False):
    cols_import = ['Winning', 'Invoer_nationaal', 'Invoer_internationaal']
    cols_export = ['Uitvoer_nationaal', 'Uitvoer_internationaal']
    rme_matrices_file = 'geoFluxus/CBS_to_RME.xlsx'
    cbs_rme = pd.read_excel(f'{FILEPATH}/{rme_matrices_file}', sheet_name='CBS_to_RME_codes').fillna(0)
    eur_or_t = pd.read_excel(f'{FILEPATH}/{rme_matrices_file}', sheet_name='eur_or_t')
    eur_or_t.set_index(eur_or_t['CBS_name'])

    #Load conversion tables
    rme_import_coefficients = pd.read_excel(f'{FILEPATH}/{rme_matrices_file}', sheet_name='RME_import_'+str(year))
    rme_export_coefficients = pd.read_excel(f'{FILEPATH}/{rme_matrices_file}', sheet_name='RME_export_'+str(year))
    rm_groups_import = rme_import_coefficients['Raw_material_name'][1:]
    rm_groups_export = rme_export_coefficients['Raw_material_name'][1:]

    #Compute conversion matrices (between CBS and RM groups)
    convert_import = cbs_rme.values[1:, 1:].astype(float) @ rme_import_coefficients.values[1:, 2:].T
    converter_import = pd.DataFrame(index=cbs_rme['CBS_name'][1:], columns=rm_groups_import, data=convert_import)

    convert_export = cbs_rme.values[1:, 1:].astype(float) @ rme_export_coefficients.values[1:, 2:].T
    converter_export = pd.DataFrame(index=cbs_rme['CBS_name'][1:], columns=rm_groups_export, data=convert_export)
    if save:
        converter_import.to_excel(FILEPATH + f'cbs_to_rme_conversion_table_import_{year}.xlsx')
        converter_export.to_excel(FILEPATH + f'cbs_to_rme_conversion_table_export_{year}.xlsx')
    rm_data = pd.DataFrame()

    #Add keys for which groups use monitary values, and which use tons.
    df = pd.merge(df, eur_or_t,left_on='Goederengroep', right_on='CBS_name', how='left')
    eur_df = pd.merge(eur_df, eur_or_t, left_on='Goederengroep', right_on='CBS_name', how='left')
    rm_data[['Jaar', 'Regionaam', 'Goederengroep']] = df[['Jaar','Regionaam', 'Goederengroep']]
    rm_data.set_index(['Jaar', 'Regionaam', 'Goederengroep'], inplace=True)
    df.set_index(['Jaar','Regionaam', 'Goederengroep'], inplace=True)
    eur_df.set_index(['Jaar','Regionaam', 'Goederengroep'], inplace=True)

    #Fill euros where the conversion table asks for monetary values, and tons where tons
    for i in range(len(cols_import)):
        rm_data[cols_import[i]] = eur_df['eur'] * eur_df[cols_import[i]] + df['ton'] * df[cols_import[i]]
    for i in range(len(cols_export)):
        rm_data[cols_export[i]] = eur_df['eur'] * eur_df[cols_export[i]] + df['ton'] * df[cols_export[i]]
    rm_data.reset_index(inplace=True)
    df.reset_index(inplace=True)
    eur_df.reset_index(inplace=True)

    #Now convert the euro / tonne values per CBS category to raw material tons
    df_import = pd.merge(rm_data, converter_import, left_on='Goederengroep', right_index=True, how='left')
    df_import.fillna(0, inplace=True)
    #First create all the columns for import categories and raw material combinations
    out_cols = set()
    for i in rm_groups_import:
        for j in cols_import:
            out_cols.add((i, j))
    out_cols = list(out_cols)
    df_import = pd.concat([df_import, pd.DataFrame(0, columns=out_cols, index = df_import.index)], axis=1)

    #Add results to each row, check if there are duplicate values and if so, first sum
    for i in rm_groups_import:
        for j in cols_import:
            df_import[i, j] += df_import[i] * df_import[j]


    materials = df_import.groupby(['Regionaam', 'Jaar'])[out_cols].sum()
    materials.columns = pd.MultiIndex.from_tuples(out_cols)
    materials = materials.stack(level=0, future_stack=True)
    if abiotisch:
        #print(materials.index.get_level_values(2))
        abiotics = pd.read_excel(f'{FILEPATH}/{rme_matrices_file}', sheet_name='abiotisch')
        materials = materials[materials.index.get_level_values(2).isin(abiotics['Abiotisch'])]
    materials['RMI'] = materials['Winning'] + materials['Invoer_internationaal'] + materials['Invoer_nationaal']
    df_import = None


    df_export = pd.merge(rm_data, converter_export, left_on='Goederengroep', right_index=True, how='left')
    df_export.fillna(0, inplace=True)
    out_cols = set()
    for i in rm_groups_export:
        for j in cols_export:
            out_cols.add((i, j ))
    out_cols = list(out_cols)
    df_export = pd.concat([df_export, pd.DataFrame(0, columns=out_cols, index = df_export.index)], axis=1)

    for i in rm_groups_export:
        for j in cols_export:
            df_export[i, j] += df_export[i] * df_export[j]


    materials_export = df_export.groupby(['Regionaam', 'Jaar'])[out_cols].sum()
    materials_export.columns = pd.MultiIndex.from_tuples(out_cols)
    materials_export = materials_export.stack(level=0, future_stack=True)
    if abiotisch:
        # print(materials.index.get_level_values(2))
        #abiotics = pd.read_excel(FILEPATH + rme_matrices_file, sheet_name='abiotisch')
        materials_export = materials_export[materials_export.index.get_level_values(2).isin(abiotics['Abiotisch'])]

    materials = pd.merge(materials, materials_export, left_index=True, right_index=True, how='outer')
    materials['RMC'] = materials['RMI'] - materials['Uitvoer_nationaal'] - materials['Uitvoer_internationaal']
    return materials.reset_index()


def calculate_indicators(path, file_name, corop=var.COROPS, raw_materials=False, goal='abiotisch'):
    dmcs = pd.DataFrame()
    dmis = pd.DataFrame()
    all_data = pd.DataFrame()
    all_eur_data = pd.DataFrame()
    all_raw_data = pd.DataFrame()
    all_rm_data = pd.DataFrame()
    if raw_materials:
        eur_all_data = pd.DataFrame()
        eur_all_raw_data = pd.DataFrame()
        rmis = pd.DataFrame()
        rmcs = pd.DataFrame()

    # import csb goederen
    df = pd.read_csv(path + file_name, low_memory=False, sep=',')
    df = df.dropna(how='all', axis='columns')

    # iterate all years
    for year in var.DMI_YEARS:
        # filter by year & COROPS
        # exclude afval and total sums
        if isinstance(corop, str): corop = [corop]
        df_year = df[
            (df['Jaar'] == year) &
            (df['Regionaam'].isin(corop)) &
            (~df['Goederengroep_naam'].str.contains('afval', case=False, na=False)) &
            (df['Gebruiksgroep_naam'] != 'Totaal')
        ].copy()

        # lokale winning
        lokale_winning_groups = RESOURCE_TYPE[RESOURCE_TYPE['Lokale winning'] == 'ja']
        lokale_winning_groups = lokale_winning_groups['Goederengroep'].tolist()

        # compute local extraction
        data = compute_local_extraction(df_year, value="Brutogew",
                                        lokale_winning_groups=lokale_winning_groups)

        if raw_materials:
            eur_data = compute_local_extraction(df_year, value="Waarde",
                                                lokale_winning_groups=lokale_winning_groups)

        # if required by the goal, include only abiotic product groups
        if 'abiotisch' in goal:
            abiotisch = data[data['Grondstof'] == 'abiotisch']

            # there are only two gemengd categories, assuming equal distribution
            abiotisch_in_gemengd = data[data['Grondstof'] == 'gemengd']
            abiotisch_in_gemengd = abiotisch_in_gemengd.apply(lambda x: x * 0.5 if x.dtype == 'float64' else x)

            all_abiotisch = pd.concat([abiotisch, abiotisch_in_gemengd])

            aggregated = all_abiotisch.groupby(['Regionaam']).sum().reset_index()
            if raw_materials:
                #Assume that we don't aggregate data
                rm_data = data.copy()
                eur_aggregated = eur_data.copy()
        # if required by the goal, aggregate per RESOURCE_TYPE type biotic/abiotic/mixed
        elif goal == 'agg_per_type':
            aggregated = data.groupby(['Regionaam', 'Grondstof']).sum().reset_index()
            if raw_materials:
                eur_aggregated = eur_data.groupby(['Regionaam', 'Grondstof']).sum().reset_index()
        # aggregated per province
        elif goal == 'agg_per_province':
            aggregated = data.groupby(['Regionaam']).sum().reset_index()
            if raw_materials:
                eur_aggregated = eur_data.groupby(['Regionaam']).sum().reset_index()
        # not aggregated at all
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
        #print(outcomes_rm.columns)
        dmc = aggregated[['Regionaam', 'DMC', 'Jaar']].copy(deep=True)
        dmi = aggregated[['Regionaam', 'DMI', 'Jaar']].copy(deep=True)

        if raw_materials:
            rmc = outcomes_rm[['Regionaam', 'RMC', 'Jaar']].copy(deep=True)
            rmi = outcomes_rm[['Regionaam', 'RMI', 'Jaar']].copy(deep=True)
        # prepare dataframes for visualisation or exports
        dmcs = pd.concat([dmcs, dmc])
        dmis = pd.concat([dmis, dmi])

        if raw_materials:
            rmcs = pd.concat([rmcs, rmc])
            rmis = pd.concat([rmis, rmi])
            all_rm_data = pd.concat([all_rm_data, outcomes_rm])
            all_eur_data = pd.concat([all_eur_data, eur_aggregated])
        all_data = pd.concat([all_data, aggregated])

    if raw_materials:
        return dmcs, dmis, rmcs, rmis, all_data, all_eur_data, all_rm_data
    else:
        return dmcs, dmis


def run():
    global FILEPATH, RESOURCE_TYPE

    print("\nDMI-RMI")
    FILEPATH = f"{var.INPUT_DIR}/Database_LockedFiles/DATA/monitor_data/data"

    # import csb data
    # stromen -> million kg
    filename = f"/CBS/{var.COROP_FILE}.csv"

    # read division into biotic / abiotic product groups
    path = f"{FILEPATH}/geofluxus"
    RESOURCE_TYPE = pd.read_csv(f'{path}/cbs_biotisch_abiotisch_2024_final.csv', delimiter=';')

    # Call the calculate indicators function with raw material calculations enabled.
    dmcs, dmis, rmcs, rmis, all_data, all_eur_data, all_rm_data = calculate_indicators(FILEPATH, filename,
                                                            raw_materials=True,
                                                            goal='total')
    # export all data for later analysis
    all_rm_data.to_excel(f'{var.OUTPUT_DIR}/all_raw_material_data.xlsx')
    all_data.to_excel(f'{var.OUTPUT_DIR}/all_data.xlsx')
    all_eur_data.to_excel(f'{var.OUTPUT_DIR}/euro_data_all.xlsx')

    dmcs_ab, dmis_ab, rmcs_ab, rmis_ab, _, _, _ = calculate_indicators(FILEPATH, filename,
                                                                 raw_materials=True)

    # export dmi/dmc graphs
    sheets = {
        'dmc': dmcs,
        'dmi': dmis,
        'rmc': rmcs,
        'rmi': rmis,
        'dmc_ab': dmcs_ab,
        'dmi_ab': dmis_ab,
        'rmc_ab': rmcs_ab,
        'rmi_ab': rmis_ab
    }
    with pd.ExcelWriter(f"{var.OUTPUT_DIR}/dmi_dmc.xlsx", engine="openpyxl") as writer:
        for indicator, data in sheets.items():
            data.to_excel(writer, sheet_name=indicator, index=False)


if __name__ == "__main__":
    run()

