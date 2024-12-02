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


def compute_local_extraction(data, value=None, lokale_winning_groups=None):
    # pivot table for analysis
    data = pd.pivot_table(
        data,
        values=[value],
        columns=["Stroom"],
        index=['Regionaam', "Goederengroep_naam"],
        aggfunc="sum",
        fill_value=0
    ).reset_index()
    data.columns = [
        col[0]
        if col[0] != value
        else col[1] for col in data.columns
    ]
    data = data.rename(columns={'Goederengroep_naam': 'Goederengroep'})

    # compute local extraction (lokale winning)
    lokale_winning = data[data['Goederengroep'].isin(lokale_winning_groups)].copy(deep=True)
    lokale_winning['Winning'] = (lokale_winning['Uitvoer_nationaal'] +
                                 lokale_winning['Uitvoer_internationaal'] +
                                 lokale_winning['Aanbod_eigen_regio'])
    lokale_winning = lokale_winning[['Regionaam', 'Goederengroep', 'Winning']]
    data = pd.merge(data, lokale_winning, how='left', on=['Regionaam', 'Goederengroep'])
    data.fillna(0, inplace=True)
    data = data.merge(RESOURCE_TYPE, on='Goederengroep')

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


def calculate_indicators(path, file_name, raw_materials=False, goal='abiotisch'):
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
        df_year = df[
            (df['Jaar'] == year) &
            (df['Regionaam'].isin(var.COROPS)) &
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
        return dmcs, dmis, rmcs, rmis
    else:
        return dmcs, dmis


def regression(func, fig, *args, **kwargs):
    """ replication of seaborn function for regression analysis.
    This function produces identical graphs to the one provided by seaborn.
    In addition it prints out the lower and higher bounds of the confidence polygons.
    These bounds are used to define the confidence interval of the regression. """

    # column name
    col = args[1]

    # If color was a keyword argument, grab it here
    kw_color = kwargs.pop("color", None)

    # How we use the function depends on where it comes from
    func_module = str(getattr(func, "__module__", ""))

    # Iterate over the data subsets
    for (row_i, col_j, hue_k), data_ijk in fig.facet_data():
        # If this subset is null, move on
        if not data_ijk.values.size:
            continue

        # Get the current axis
        modify_state = not func_module.startswith("seaborn")
        ax = fig.facet_axis(row_i, col_j, modify_state)

        # Decide what color to plot with
        kwargs["color"] = fig._facet_color(hue_k, kw_color)

        # Insert the other hue aesthetics if appropriate
        for kw, val_list in fig.hue_kws.items():
            kwargs[kw] = val_list[hue_k]

        # Insert a label in the keyword arguments for the legend
        if fig._hue_var is not None:
            kwargs["label"] = fig.hue_names[hue_k]

        # Get the actual data we are going to plot with
        plot_data = data_ijk[list(args)]
        if fig._dropna:
            plot_data = plot_data.dropna()
        plot_args = [v for k, v in plot_data.items()]

        # Some matplotlib functions don't handle pandas objects correctly
        if func_module.startswith("matplotlib"):
            plot_args = [v.values for v in plot_args]

        # Draw the plot
        if str(func.__module__).startswith("seaborn"):
            plot_kwargs = kwargs.copy()
            semantics = ["x", "y", "hue", "size", "style"]
            for key, val in zip(semantics, plot_args):
                plot_kwargs[key] = val
            plot_args = []

        # regression
        plotter = _RegressionPlotter(*plot_args, **plot_kwargs)
        grid, yhat, err_bands = plotter.fit_regression(ax)

        return yhat, err_bands


def visualise_per_province(data, indicator=None):
    for goal, goal_data in data.items():
        viz_data = goal_data.groupby(['Regionaam', 'Jaar']).sum().reset_index()

        sns.set()
        fig = sns.FacetGrid(data=viz_data, col='Regionaam', hue='Regionaam', aspect=0.5, height=5, col_wrap=6)
        fig.set(xlim=(PROJ_START, PROJ_END))

        yhat, err_bands = regression(sns.regplot, fig, "Jaar", indicator, truncate=False)
        goal_value = viz_data[viz_data['Jaar'] == PROJ_START + 1][indicator].values[0] / 2

        item = DATA.setdefault(indicator, {})
        item[goal.lower()] = {
            "points": {
                "period": viz_data["Jaar"].to_list(),
                "values": viz_data[indicator].to_list()
            },
            "line": {
                "x1": PROJ_START,
                "y1": yhat[0],
                "x2": PROJ_END,
                "y2": yhat[-1]
            },
            "area": {
                "upper_bound": err_bands[0].tolist(),
                "lower_bound": err_bands[1].tolist()
            },
            **({'goal': {
                "x1": PROJ_START,
                "y1": goal_value,
                "x2": PROJ_END,
                "y2": goal_value
            }} if goal == 'abiotic' else {})
        }


def run():
    global FILEPATH, RESOURCE_TYPE

    print("\nDMI-RMI")
    FILEPATH = f"{var.INPUT_DIR}/DATA/monitor_data/data"

    # import csb data
    # stromen -> million kg
    filename = f"/CBS/{var.COROP_FILE}.csv"

    # read division into biotic / abiotic product groups
    path = f"{FILEPATH}/geofluxus"
    RESOURCE_TYPE = pd.read_csv(f'{path}/cbs_biotisch_abiotisch_2024_final.csv', delimiter=';')

    # Call the calculate indicators function with raw material calculations enabled.
    dmcs, dmis, rmcs, rmis = calculate_indicators(FILEPATH, filename,
                                                  raw_materials=True,
                                                  goal='total')

    dmcs_ab, dmis_ab, rmcs_ab, rmis_ab = calculate_indicators(FILEPATH, filename,
                                                              raw_materials=True)

    indicators = {
        'DMC': {'total': dmcs, 'abiotic': dmcs_ab},
        'DMI': {'total': dmis, 'abiotic': dmis_ab},
        'RMC': {'total': rmcs, 'abiotic': rmcs_ab},
        'RMI': {'total': rmis, 'abiotic': rmis_ab}
    }
    for indicator, data in indicators.items():
        visualise_per_province(data, indicator=indicator)

    return DATA



