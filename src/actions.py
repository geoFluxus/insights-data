import pandas as pd
import geopandas as gpd
from sklearn.linear_model import LinearRegression


def compute_trends():
    """
    Compute trends via Linear Regression
    """


def compute_actions(flows, provincies, gemeenten):
    """
    Compute actions for provincie & gemeenten
    """

    # add gemeente to flow origins
    flows['wkt'] = gpd.GeoSeries.from_wkt(flows['Ontdoener_Location'])
    flows = gpd.GeoDataFrame(flows, geometry='wkt', crs={"init": "epsg:4326"})
    flows = gpd.sjoin(flows, gemeenten, how='left', op='within')
    print(flows)