import matplotlib.pyplot as plt
import pandas as pd
import variables as var
col = [(235, 172, 35), (184, 0, 88), (0, 140, 249), (0, 110, 0), (0, 187, 173), (209, 99, 230), (178, 69, 2), (255, 146, 135), (89, 84, 214), (0, 198, 248), (135, 133, 0), (0, 167, 108), (189, 189, 189)]
cols = []
for i in range(len(col)):
    c = (col[i][0]/255,col[i][1]/255,col[i][2]/255)
    cols.append(c)

goederengroep_colors = {
    "Andere stoffen van plantaardige oorsprong": "#1f77b4",  # Blue
    "Betonwaren en overige bouwmaterialen en bouwproducten": "#ff7f0e",  # Orange
    "Chemische basisproducten": "#2ca02c",  # Green
    "Dierlijke en plantaardige oliën en vetten": "#d62728",  # Red
    "Gasvormige aardolieproducten": "#9467bd",  # Purple
    "Granen": "#9edae5",  # Brown
    "IJzererts": "#e377c2",  # Pink
    "Kantoormachines en computers": "#7f7f7f",  # Gray
    "Kunstmeststoffen en stikstofverbindingen (behalve natuurlijke meststoffen)": "#bcbd22",  # Yellow-green
    "Levende dieren": "#17becf",  # Cyan
    "Maalderijproducten, zetmeel en zetmeelproducten; bereide diervoeders": "#aec7e8",  # Light Blue
    "Non-ferrometalen en producten daarvan": "#ffbb78",  # Light Orange
    "Rauwe melk van runderen, schapen en geiten": "#98df8a",  # Light Green
    "Ruwe aardolie": "#ff9896",  # Light Red
    "Steenkool en bruinkool": "#c5b0d5",  # Light Purple
    "Televisie- en radiotoestellen, audio- en videoapparatuur (bruingoed)": "#c49c94",  # Light Brown
    "Vlees, ongelooide huiden en vellen en vleesproducten": "#f7b6d2",  # Light Pink
    "Vloeibare aardolieproducten": "#dbdb8d",  # Light Yellow-green
    "Zout": "#9edae5",  # Light Cyan
    "Zuivelproducten en consumptie-ijs": "#9ACD32",  # Blue (reused for consistency)

    "Aardappelen": "#FFB6C1",  # Saddle Brown
    "Andere grondstoffen van dierlijke oorsprong": "#A52A2A",  # Brown
    "Andere transportmiddelen": "#228B22",  # Forest Green
    "Andere verse groenten en vers fruit": "#32CD32",  # Lime Green
    "Buizen, pijpen en holle profielen van ijzer en staal": "#708090",  # Slate Gray
    "Cement, kalk en gips": "#D3D3D3",  # Light Gray
    "Cokes en vaste aardolieproducten": "#696969",  # Dim Gray
    "Dranken": "#FFD700",  # Gold
    "Drukwerk en opgenomen media": "#8A2BE2",  # Blue Violet
    "Elektronische onderdelen en zend- en transmissietoestellen": "#4682B4",  # Steel Blue
    "Farmaceutische producten en chemische specialiteiten": "#FF4500",  # Orange Red
    "Glas en glaswerk, keramische producten": "#00CED1",  # Dark Turquoise
    "Groenten en fruit, verwerkt en verduurzaamd": "#9ACD32",  # Yellow Green
    "Hout- en kurkwaren (m.u.v. meubelen)": "#FFA07A",  # Peru
    "Huishoudapparaten (witgoed)": "#D2691E",  # Chocolate
    "IJzer en staal en halffabricaten daarvan (behalve buizen)": "#708090",  # Slate Gray
    "Ketels, ijzerwaren, wapens en andere producten van metaal": "#8B0000",  # Dark Red
    "Kleding en artikelen van bont": "#FF69B4",  # Hot Pink
    "Kunststoffen en synthetische rubber in primaire vormen": "#00FA9A",  # Medium Spring Green
    "Leder en lederwaren": "#8B4513",  # Saddle Brown
    "Levende planten en bloemen": "#32CD32",  # Lime Green
    "Machines en werktuigen voor de land- en bosbouw": "#556B2F",  # Dark Olive Green
    "Medische, precisie- en optische instrumenten": "#8A2BE2",  # Blue Violet
    "Metalen constructieproducten": "#2F4F4F",  # Dark Slate Gray
    "Meubilair": "#DEB887",  # Burly Wood
    "Mineralen voor de chemische en kunstmestindustrie": "#696969",  # Steel Blue
    "Non-ferrometaalertsen": "#B0C4DE",  # Light Steel Blue
    "Overige elektrische machines en apparaten": "#FF6347",  # Tomato
    "Overige goederen": "#A9A9A9",  # Dark Gray
    "Overige machines en gereedschapswerktuigen": "#20B2AA",  # Light Sea Green
    "Overige voedingsmiddelen en tabaksproducten": "#FFA07A",  # Light Salmon
    "Producten van de auto-industrie": "#DC143C",  # Crimson
    "Producten van de bosbouw": "#8B4513",  # Saddle Brown
    "Producten van rubber of kunststof": "#00FA9A",  # Medium Spring Green
    "Pulp, papier en papierwaren": "#B0E0E6",  # Powder Blue
    "Steen, zand, grind, klei, turf en andere delfstoffen": "#808080",  # Gray
    "Suikerbieten": "#FFDAB9",  # Peach Puff
    "Textiel": "#FFB6C1",  # Light Pink
    "Vis en andere visserijproducten": "#4682B4",  # Steel Blue
    "Vis en visproducten, verwerkt en verduurzaamd": "#5F9EA0"  # Cadet Blue
}

def environmental_time_plot(data, area_type = 'Provincie', area_name='Friesland', indicator='CO2', normalize=False, show=False):
    plt.close()
    if indicator == 'CO2':
        ind_index = 1
    elif indicator == 'MKI':
        ind_index = 0
    else:
        ind_index = 0

    label_names = ['Milieukostenindicator (mln euro)', 'CO2eq uitstoot (kt)', 'Domestic Material Input']
    col_names = ['MKI total (mln euro)', 'CO2 emissions total (kt)', 'DMI']
    viz_data = data.groupby([area_type, 'Jaar'])[[col_names[0], col_names[1]]].sum().reset_index()
    viz_data = viz_data[viz_data[area_type] == area_name]
    viz_data['Jaar'] = viz_data['Jaar'].astype(int)
    if normalize:
        for i in range(2):
            reference_val = viz_data[viz_data['Jaar'] == 2015][col_names[i]].values[0]
            viz_data[col_names[i]] = 100 * viz_data[col_names[i]] / reference_val
        fig = viz_data.plot.line(x='Jaar', y=[col_names[0], col_names[1]])
    else:
        fig = viz_data.plot.line(x='Jaar', y=col_names[ind_index], color=cols[0], linestyle='', marker='o', legend=False, figsize=(8,7))
        fig.set(ylabel=label_names[ind_index])
    fig.set(xlim=(2015, 2024), ylim=(0, None))
    ymin, ymax = fig.get_ylim()

    # Calculate new limits with a 10% increase
    range_padding = 0.1 * (ymax - ymin)
    new_ymax = ymax + range_padding
    # Set the new y-axis limits
    fig.set(ylim=(0, new_ymax))
    if show: plt.show()
    else:
        plt.savefig(f'../{var.OUTPUT_DIR}/{label_names[ind_index]} time plot {area_name}.png', dpi=200)
        viz_data.drop(columns=col_names[0]).to_excel(f'../{var.OUTPUT_DIR}/{label_names[ind_index]} data.xlsx', index=False)


def environmental_bar_plot(data, year=2023, indicator='CO2', area_type='Provincie', area_name='Friesland', normalize=False, threshold=0.82,
                          show=False) :

    plt.close()
    if indicator == 'CO2' :
        ind_index = 1
    elif indicator == 'MKI' :
        ind_index = 0
    else :
        ind_index = 0

    # Define column names and labels
    label_names = ['Milieukostenindicator (mln euro)', 'CO2eq uitstoot (kt)', 'Domestic Material Input']
    col_names = ['MKI total (mln euro)', 'CO2 emissions total (kt)', 'DMI']

    # Group data and filter for the specified province and year
    viz_data = data.groupby([area_type, 'Jaar', 'Goederengroep'])[col_names].sum().reset_index()
    viz_data = viz_data[(viz_data['Jaar'] == year) & (viz_data[area_type] == area_name)]

    # Normalize data if required
    if normalize :
        for i in col_names :
            viz_data[i] = viz_data[i] / viz_data[i].sum() * 100

    # Sort values by the selected indicator
    viz_data.sort_values(by=col_names[ind_index], ascending=False, inplace=True)

    if threshold is not None :
        # Calculate cumulative sum and threshold value
        viz_data['cumulative_sum'] = viz_data[col_names[ind_index]].cumsum()
        total_sum = viz_data[col_names[ind_index]].sum()
        threshold_val = threshold * total_sum

        #print(viz_data[(viz_data['cumulative_sum'] > threshold_val)].min())
        # Separate data above and below threshold
        main_data = viz_data[(viz_data['cumulative_sum'] <= threshold_val)]
        other_data = viz_data[(viz_data['cumulative_sum'] > threshold_val)]

        # Combine other data into one category "Overig"
        if not other_data.empty :
            others_sum = other_data[col_names[ind_index]].sum()
            others_row = {'Goederengroep' : 'Overig', col_names[ind_index] : others_sum}
            main_data = pd.concat([main_data, pd.DataFrame([others_row])], ignore_index=True)

        # Drop cumulative sum column
        main_data = main_data.drop(columns=['cumulative_sum'])
    else :
        main_data = viz_data

    # Calculate percentages
    main_data['percentage'] = (main_data[col_names[ind_index]] / main_data[col_names[ind_index]].sum()) * 100

    # Reverse the order so the largest value is on top
    main_data = main_data.iloc[: :-1]

    # Create the bar plot
    colors = [goederengroep_colors.get(item, "#C1C1C1") for item in main_data['Goederengroep']]
    fig = main_data.plot(x='Goederengroep', y=col_names[ind_index], kind='barh', figsize=(10, 10), color=colors,
                         legend=False)
    labels = fig.get_yticklabels()
    labels = [i.get_text() if len(i.get_text()) < 37 else i.get_text()[:37] + '..' for i in labels]
    fig.set_yticklabels(labels, fontsize=13)
    fig.set_ylabel('Goederengroep', fontsize=13)
    plt.xlabel(label_names[ind_index], fontsize=13)
    fig.tick_params(labelsize=13)

    # Add percentage labels to bars
    _, xmax = fig.get_xlim()
    for index, value in enumerate(main_data[col_names[ind_index]]) :
        plt.text(value+xmax*0.01 if value<0.8*xmax else value-xmax*0.1, index,
                 f'{main_data["percentage"].iloc[index]:.1f}%', va='center')

    plt.tight_layout()

    # Show or save the plot
    if show :
        plt.show()
    else :
        plt.savefig(f'../{var.OUTPUT_DIR}/{label_names[ind_index]} bar plot {area_name}.png', dpi=200)

if __name__ == '__main__':
    dat = pd.read_excel(f'../{var.OUTPUT_DIR}/all_impact_data.xlsx')
    environmental_time_plot(dat, area_type='Regionaam', area_name=var.COROPS[0])
    environmental_bar_plot(dat, area_type='Regionaam', area_name=var.COROPS[0], year=var.YEAR)