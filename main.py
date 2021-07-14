import variables as var
import pandas as pd
import geopandas as gpd
from src import actions
import logging


def get_flows():
    """
    Import flows from 'data/flows'
    """
    flows = []
    for year in var.YEARS:
        filename = f'./data/flows/ontvangst_{var.PROVINCE.lower()}_{year}.csv'
        csv = pd.read_csv(filename, low_memory=False)
        flows.append(csv)

    return pd.concat(flows)


def get_areas():
    """
    Import areas from 'data/areas'
    """
    provincies = gpd.read_file('./data/areas/provincies.shp')
    gemeenten = gpd.read_file('./data/areas/gemeenten.shp')
    return provincies, gemeenten


if __name__ == '__main__':
    # logging: timestamp, warning level & message
    logging.basicConfig(filename="logs.txt",  # file name
                        filemode="w",  # overwrite
                        level=logging.INFO,  # lowest warning level
                        format="%(asctime)s [%(levelname)s]: %(message)s" # message format
                        )

    # start analysis
    logging.info("START ANALYSIS...\n")

    # get flows
    logging.info("INPUTS...")
    logging.info("Load flows...")
    # flows = get_flows()
    flows = pd.read_csv(f'./data/flows/ontvangst_{var.PROVINCE.lower()}.csv', low_memory=False)
    flows['Gewicht_TN'] = flows['Gewicht_KG'] / 10**3
    logging.info(f"Total flows: {len(flows)}")

    # get areas
    logging.info("Load areas...\n")
    provincies, gemeenten = get_areas()

    # actions
    logging.info("ANALYSE ACTIONS...")
    actions.compute_actions(flows, provincies, gemeenten)

    # ebd analysis
    logging.info("END ANALYSIS...\n")