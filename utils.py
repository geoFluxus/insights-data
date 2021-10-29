import pandas as pd
import geopandas as gpd
import numpy as np


def import_areas(level=None):
    """
    import area polygons
    for different administration level
    (municipalities, provinces etc.)
    """
    # load geometries
    areas = gpd.read_file(f'./data/areas/{level}.shp')
    areas['centroid'] = areas['geometry'].centroid

    return areas


def add_areas(flows, areas=None, role=None, admin_level=None):
    """
    Add administrative areas to roles
    (point to polygon)
    """
    columns = list(flows)

    # join geolocation with area polygons
    flows['wkt'] = gpd.GeoSeries.from_wkt(flows[f'{role}_Location'])
    flows = gpd.GeoDataFrame(flows, geometry='wkt', crs='EPSG:4326')
    flows = gpd.sjoin(flows, areas, how='left', op='within')

    # keep only original dataframe columns & role area
    flows[f'{role}_{admin_level}'] = flows['name']
    columns.append(f'{role}_{admin_level}')
    flows = flows[columns]

    return flows


def add_classification(df, classif, name=None,
                       left_on=None, right_on=None):
    """
    Add ontology classifications
    to LMA & CBS data
    """
    classif[right_on] = classif[right_on].astype(str).str.zfill(6)
    df[left_on] = df[left_on].astype(str).str.zfill(6)
    df = pd.merge(df, classif, how='left', left_on=left_on, right_on=right_on)
    df.loc[df[name].isnull(), name] = 'unknown'
    return df


def compute_sankey_branch(flows,
                          source=None, source_in=True,
                          target=None, target_in=True,
                          level=None, areas=[]):
    """
    Compute sankey brances
    for LMA & CBS data
    """
    binnen = {
        True: 'binnen',
        False: 'buiten'
    }
    conditions = []
    for area in areas:
        for role, is_in in zip([source, target], [source_in, target_in]):
            condition = flows[f'{role}_{level}'].isin([area])
            if not is_in: condition = ~condition
            conditions.append(condition)
        new_flows = flows[np.bitwise_and.reduce(conditions)]
        new_flows = new_flows['Gewicht_KG'].sum() / 10**9  # megatonnes
        print(f'{source} ({binnen[source_in]} {area}) -> {target} ({binnen[target_in]} {area}): {new_flows} Mtn')


def get_classification_graphs(df, source=None,
                              level=None, area=None, klass=None):
    """
    Create graphs based on ontology classifications
    for LMA & CBS data
    """
    categories = {
        'chains': [
            'primair',
            'secundair',
            'tertiair',
            'quartair',
            'unknown'
        ]
    }

    groupby = []

    flows = df.copy()
    if source is not None:
        # filter source in areas
        flows = flows[flows[f'{source}_{level}'].isin([area])]
        flows = flows.rename(columns={source: 'source'})

        groupby.append(f'{source}_{level}',)

    # groupby: source, materials
    groupby.extend([
        klass,
        'Gewicht_KG'
    ])
    groups = flows[groupby].groupby(groupby[:-1]).sum().reset_index()

    # specify categories
    results = []
    values = []
    for cat in categories[klass]:
        row = groups[groups[klass] == cat]
        value = row['Gewicht_KG'].values[0] if len(row) else 0
        values.append(round(value / 10**9, 2))  # kg -> Mt

    # add to results
    results.append({
        "name": area,
        klass: categories[klass],
        "values": {
            "weight": {
                "value": values,
                "unit": "Mt"
            }
        }
    })

    return results