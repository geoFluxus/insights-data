from variables import *
import pandas as pd
import geopandas as gpd
from src import actions


def get_flows():
    """
    Import flows from 'data/flows'
    """
    flows = []
    for year in YEARS:
        filename = f'./data/flows/ontvangst_{PROVINCE.lower()}_{year}.csv'
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
    # get flows
    flows = get_flows()

    # get areas
    provincies, gemeenten = get_areas()

    # actions
    actions.compute_actions(flows, provincies, gemeenten)