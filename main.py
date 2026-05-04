import variables as var
from src import dmi_dmc
from src.analysis import benchmark, environmental_cost, material_heatmap
import importlib
import os
import pandas as pd


regions = [
    ("Oost-Groningen", "Oost-Groningen"),
    ("Delfzijl e.o.", "Delfzijl en omgeving"),
    ("Overig Groningen", "Overig Groningen"),
    ("Noord-Friesland", "Noord-Friesland"),
    ("Zuidwest-Friesland", "Zuidwest-Friesland"),
    ("Zuidoost-Friesland", "Zuidoost-Friesland"),
    ("Noord-Drenthe", "Noord-Drenthe"),
    ("Zuidoost-Drenthe", "Zuidoost-Drenthe"),
    ("Zuidwest-Drenthe", "Zuidwest-Drenthe"),
    ("Noord-Overijssel", "Noord-Overijssel"),
    ("Zuidwest-Overijssel", "Zuidwest-Overijssel"),
    ("Twente", "Twente"),
    ("Veluwe", "Veluwe"),
    ("Achterhoek", "Achterhoek"),
    ("Aggl. Arnhem/Nijmegen", "Arnhem/Nijmegen"),
    ("Zuidwest-Gelderland", "Zuidwest-Gelderland"),
    ("Utrecht-West", "Utrecht-West"),
    ("Stadsgewest Amersfoort", "Stadsgewest Amersfoort"),
    ("Stadsgewest Utrecht", "Stadsgewest Utrecht"),
    ("Zuidoost-Utrecht", "Zuidoost-Utrecht"),
    ("Kop van Noord-Holland", "Kop van Noord-Holland"),
    ("Alkmaar e.o.", "Alkmaar en omgeving"),
    ("IJmond", "IJmond"),
    ("Agglomeratie Haarlem", "Agglomeratie Haarlem"),
    ("Zaanstreek", "Zaanstreek"),
    ("Amsterdam", "Amsterdam"),
    ("Overig Agglomeratie Amsterdam", "Overig Agglomeratie Amsterdam"),
    ("Edam-Volendam e.o.", "Edam-Volendam en omgeving"),
    ("Haarlemmermeer e.o.", "Haarlemmermeer en omgeving"),
    ("Het Gooi en Vechtstreek", "Het Gooi en Vechtstreek"),
    ("Agglomeratie Leiden en Bollenstreek", "Agglomeratie Leiden en Bollenstreek"),
    ("Agglomeratie’s-Gravenhage (Excl. Zoetermeer)", "Agglomeratie 's-Gravenhage exclusief Zoetermeer"),
    ("Zoetermeer", "Zoetermeer"),
    ("Delft en Westland", "Delft en Westland"),
    ("Oost-Zuid-Holland", "Oost-Zuid-Holland"),
    ("Rijnmond", "Rijnmond"),
    ("Overig Groot-Rijnmond", "Overig Groot-Rijnmond"),
    ("Drechtsteden", "Drechtsteden"),
    ("Overig Zuidoost-Zuid-Holland", "Overig Zuidoost-Zuid-Holland"),
    ("Zeeuwsch-Vlaanderen", "Zeeuwsch-Vlaanderen"),
    ("Overig Zeeland", "Overig Zeeland"),
    ("West-Noord-Brabant", "West-Noord-Brabant"),
    ("Midden-Noord-Brabant", "Midden-Noord-Brabant"),
    ("Stadsgewest ’s-Hertogenbosch", "Stadsgewest 's-Hertogenbosch"),
    ("Overig Noordoost-Noord-Brabant", "Overig Noordoost-Noord-Brabant"),
    ("Zuidoost-Noord-Brabant", "Zuidoost-Noord-Brabant"),
    ("Noord-Limburg", "Noord-Limburg"),
    ("Midden-Limburg", "Midden-Limburg"),
    ("Zuid-Limburg", "Zuid-Limburg"),
    ("Almere", "Almere"),
    ("Flevoland-Midden", "Flevoland-Midden"),
    ("Noordoostpolder en Urk", "Noordoostpolder en Urk")
]

for corop, area in regions:
    print(f'PROCESSING {area}...')
    path = fr"C:\Users\vasil\geoFluxus Dropbox\geoFluxus\Projects\Liepa\{area}"
    var.COROPS = [corop]
    var.AREA = area
    var.OUTPUT_DIR = path

    os.makedirs(path, exist_ok=True)

    importlib.reload(dmi_dmc)
    importlib.reload(environmental_cost)
    importlib.reload(material_heatmap)
    importlib.reload(benchmark)

    dmi_dmc.run()
    environmental_cost.run()
    # material_heatmap.run()
    # benchmark.run()

    # # Load all sheets
    # path = fr"C:\Users\vasil\geoFluxus Dropbox\geoFluxus\Projects\Liepa\{area}"
    # file_path = fr"{path}\all_data.xlsx"
    # df = pd.read_excel(file_path, sheet_name='ALL')
    #
    # # Select relevant columns
    # df = df.rename(columns={'Goederengroep': 'Goederengroep (kton)'})
    # df = df[['Goederengroep (kton)', 'Jaar', 'DMI', 'DMC']]
    #
    # # ---- DMI (wide) ----
    # dmi_wide = (
    #     df.groupby(['Goederengroep (kton)', 'Jaar'])['DMI']
    #         .sum()
    #         .unstack(fill_value=0)  # years become columns
    # )
    #
    # # ---- DMC (wide) ----
    # dmc_wide = (
    #     df.groupby(['Goederengroep (kton)', 'Jaar'])['DMC']
    #         .sum()
    #         .unstack(fill_value=0)
    # )
    #
    # # Optional: sort columns (years)
    # dmi_wide = dmi_wide.sort_index(axis=1)
    # dmc_wide = dmc_wide.sort_index(axis=1)
    #
    # file_path = fr"{path}\all_raw_material_data.xlsx"
    # df = pd.read_excel(file_path, sheet_name='ALL')
    #
    # # Select relevant columns
    # df = df.rename(columns={'level_2': 'Grondstofgroep(kton)'})
    # df = df[['Grondstofgroep(kton)', 'Jaar', 'RMI', 'RMC']]
    #
    # # ---- DMI (wide) ----
    # rmi_wide = (
    #     df.groupby(['Grondstofgroep(kton)', 'Jaar'])['RMI']
    #         .sum()
    #         .unstack(fill_value=0)  # years become columns
    # )
    #
    # # ---- DMC (wide) ----
    # rmc_wide = (
    #     df.groupby(['Grondstofgroep(kton)', 'Jaar'])['RMC']
    #         .sum()
    #         .unstack(fill_value=0)
    # )
    #
    # # Optional: sort columns (years)
    # rmi_wide = rmi_wide.sort_index(axis=1)
    # rmc_wide = rmc_wide.sort_index(axis=1)
    #
    # waste = pd.read_excel(fr"{path}\benchmark.xlsx")
    #
    # file_path = fr"{path}\all_impact_data.xlsx"
    # df = pd.read_excel(file_path, sheet_name='ALL')
    #
    # # Select relevant columns
    # df = df.rename(columns={'level_2': 'Grondstofgroep(kton)'})
    # df = df[['Grondstofgroep(kton)', 'Jaar', 'RMI', 'RMC']]
    #
    # # Save to Excel
    # with pd.ExcelWriter(fr"C:\Users\vasil\geoFluxus Dropbox\geoFluxus\Projects\Liepa\COROPS\{area}.xlsx") as writer:
    #     dmi_wide.to_excel(writer, sheet_name="DMI")
    #     dmc_wide.to_excel(writer, sheet_name="DMC")
    #     rmi_wide.to_excel(writer, sheet_name="RMI")
    #     rmc_wide.to_excel(writer, sheet_name="RMC")
    #     waste.to_excel(writer, sheet_name="Afval", index=False)

