import pandas as pd


def pivot_and_write(data, writer, group=None, indicators=[]):
    """
    Pivot indicators and write each into a sheet,
    prefixed to avoid name collisions.
    """
    for indicator in indicators:
        df_indi = data[['Regionaam', group, 'Jaar', indicator]]

        df_pivot = df_indi.pivot_table(
            index=['Regionaam', group],
            columns='Jaar',
            values=indicator
        ).reset_index()

        df_pivot.to_excel(writer, sheet_name=indicator, index=False)


if __name__ == '__main__':
    # ALWAYS overwrite the file cleanly
    with pd.ExcelWriter('json/general_overview.xlsx', engine='openpyxl', mode='w') as writer:

        # 1. Impact data
        impact = pd.read_excel('json/all_impact_data.xlsx')
        pivot_and_write(
            impact, writer,
            group='Goederengroep',
            indicators=['DMI', 'DMC']
        )

        # 2. Raw material data
        raw = pd.read_excel('json/all_raw_material_data.xlsx')
        pivot_and_write(
            raw, writer,
            group='level_2',
            indicators=['RMI', 'RMC']
        )

        pivot_and_write(
            impact, writer,
            group='Goederengroep',
            indicators=['CO2 emissions total (kt)', 'MKI total (mln euro)']
        )

        materials = pd.read_excel('json/material_contents.xlsx')
        materials.to_excel(writer, sheet_name='Leveringszekerheid', index=False)

        potential = pd.read_excel('json/benchmark.xlsx')
        potential['eural code'] = potential['eural code'].astype(str).str.zfill(6)
        potential.to_excel(writer, sheet_name='Afval', index=False)

    print("Finished writing general_overview.xlsx")