import variables as var
from src import dmi_dmc
from src.analysis import benchmark, environmental_cost, material_heatmap
import importlib
import os


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
    material_heatmap.run()
    benchmark.run()

