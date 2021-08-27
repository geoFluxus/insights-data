import variables as var
import pandas as pd
import geopandas as gpd
from src import actions
import logging


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
    flows = get_flows()
    flows['Gewicht_TN'] = flows['Gewicht_KG'] / 10**3
    # flows = pd.read_csv('test/temp.csv', low_memory=False)
    logging.info(f"Total flows: {len(flows)}")

    # get areas
    logging.info("Load areas...\n")
    provincies, gemeenten = get_areas()

    # actions
    logging.info("ANALYSE ACTIONS...")
    actions.compute_actions(flows, provincies, gemeenten)

    # ebd analysis
    logging.info("END ANALYSIS...\n")