import pandas as pd
import gc
import json
import numpy as np
import utils
import geopandas as gpd
import routing
from shapely import wkt
import matplotlib.pyplot as plot
import os
from sklearn.linear_model import LinearRegression


YEARS = [2016, 2017, 2018, 2019, 2020]

DATA = {}

COMPANIES = {}

NETWORK = {}


def clean_description(desc):
    """
    Apply function to clean rows from the description column
    :param desc: row from description column
    :return: formatted row
    """
    desc = desc.strip()
    desc = desc.lower()
    desc = desc.replace(u"\xa0", u" ")
    desc = " ".join(desc.split())
    if desc == "nan":
        return np.NaN
    return desc


def clean_postcode(postcode):
    """
    Apply function to clean rows from the postcode column of each role
    :param desc: row from role postcode column
    :return: formatted row
    """
    postcode = postcode.strip()
    postcode = postcode.replace(" ","")
    postcode = postcode.upper()
    if "0000" in postcode:
        return ""
    return postcode


def clean_company_name(name):
    """
    Apply function to clean rows from the company name column of each role
    :param desc: row from company name column for each column
    :return: formatted row
    """

    # remove all non-ASCII characters
    orig_name = name
    printable = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ \t\n\r\x0b\x0c"
    if isinstance(name, float) and np.isnan(name): return np.nan
    name = "".join(filter(lambda x: x in printable, name))

    name = name.upper()

    litter = [" SV", "S V", "S.V.", " BV", "B V", "B.V.", " CV", "C.V.",
              " NV", "N.V.", "V.O.F", " VOF", "V O F", "\"T", "\"S"]
    # remove all the littering characters
    for l in litter:
        name = name.replace(l, "")

    name = " ".join(name.split())

    # check if company name does not contain only digits
    name_copy = name
    for dig in "0123456789":
        name_copy = name_copy.replace(dig, "")
    if len(name_copy) == 0:
        name = ""

    return name


def clean_address(address):
    """
    Apply function to clean rows from the street/city column of each role
    :param desc: row from street/city column
    :return: formatted row
    """
    address = address.strip()
    address = address.upper()
    address = " ".join(address.split())
    return address


def clean_huisnr(nr):
    """
    Apply function to clean rows from the house number column of each role
    :param desc: row from house number column
    :return: formatted row
    """
    nr = nr.split(".")[0]
    nr = "".join(filter(lambda x: x in "0123456789", nr))
    return nr


def clean_nace(nace):
    """
    Apply function to clean NACE codes
    :param desc: row with NACE code
    :return: formatted row
    """
    nace = "".join(filter(lambda x: x in "0123456789", nace))
    return nace


def get_company_data():
    columns = [
        "Afvalstroomnummer",
        "VerwerkingsmethodeCode",
        "VerwerkingsOmschrijving",
        "RouteInzameling",
        "Inzamelaarsregeling",
        "ToegestaanbijInzamelaarsregeling",
        "EuralCode",
        "BenamingAfval",
        "MeldPeriodeJAAR",
        "MeldPeriodeMAAND",
        "Gewicht_KG",
        "Aantal_vrachten",
        "Ontdoener",
        "Ontdoener_Postcode",
        "Ontdoener_Plaats",
        "Ontdoener_Straat",
        "Ontdoener_Huisnr",
        "Ontdoener_Land",
        "Herkomst_Postcode",
        "Herkomst_Straat",
        "Herkomst_Plaats",
        "Herkomst_Huisnr",
        "Herkomst_Land",
        "Verwerker",
        "Verwerker_Postcode",
        "Verwerker_Straat",
        "Verwerker_Plaats",
        "Verwerker_Huisnr",
        "Verwerker_Land"
    ]

    # import LMA ontvangst
    for year in YEARS:
        path = '../../../../../media/geofluxus/DATA/national/ontvangst/processed'
        print(f'Import {year} data...')
        df = pd.read_csv(f'{path}/ontvangst_{year}_full.csv', usecols=columns, low_memory=False)
        df['Ontdoener_Naam'] = df['Ontdoener'].apply(clean_company_name)

        for company, names in COMPANIES.items():
            search = []

            # alternatives names
            for name in names:
                name = clean_company_name(name)
                search.append(df[df['Ontdoener_Naam'] == name])

            # # keyword
            # name = clean_company_name(company)
            # search.append(df[df['Ontdoener_Naam'].str.contains(name, na=False)])

            search = pd.concat(search).drop_duplicates()
            DATA.setdefault(company, []).append(search)

        # collect garbage
        del df
        gc.collect()

    results = {
        company: pd.concat(dfs)
        for company, dfs in DATA.items()
    }

    for fil, df in results.items():
        df.to_csv(f'afvalprofiel/{fil}.csv', index=False)


def to_wkt(lon, lat):
    return f"POINT({lon} {lat})"


def add_identifiers(df):
    df['origin'] = df['Ontdoener_Naam'] + " " + df['Herkomst_Postcode']
    df['origin'] = df['origin'].str.replace(" ", "_")
    df['destination'] = df['Verwerker_Naam'] + " " + df['Verwerker_Postcode'] + " " + df["VerwerkingsmethodeCode"]
    df['destination'] = df['destination'].str.replace(" ", "_")

    return df


def add_routings(flows, routings):
    # join with routings
    flows = pd.merge(flows, routings, how='left', on=['origin', 'destination'], indicator=True)
    unmatched = flows[flows._merge == 'left_only']
    if len(unmatched):
        columns = ['origin', 'Herkomst_Location', 'destination', 'Verwerker_Location']
        DATA.setdefault('missing_routings', []).append(unmatched[columns])
        print(f'No routings for {len(unmatched)} flows...')

    # average payload per trip
    vehicles = pd.read_excel('data/network/vehicle.xlsx')
    flows['tn'] = flows['Gewicht_KG'] / 10**3
    flows['average'] = flows['tn'] / flows['Aantal_vrachten']

    # compute co2 emissions
    for idx, row in vehicles.iterrows():
        min, max = row['min'], row['max']
        condition = (flows['average'] >= min) & (flows['average'] < max)
        flows.loc[condition, 'grams per tonne kilometer'] = row['co2']
    flows['co2'] = flows['grams per tonne kilometer'] * flows['tn'] * flows['distance'] / 10**3

    return flows


def compute_routings():
    df = pd.read_csv('afvalprofiel/missing_routings.csv', low_memory=False, sep='\t')
    con, cur = routing.open_connection()
    with open('afvalprofiel/missing_routings_processed.csv', mode='w') as f:
        f.write('origin;destination;wkt;seq\n')
        for index, row in df.iterrows():
            print(index + 1)
            origin, destination = row['origin'], row['destination']
            o_pt, d_pt = [row['Herkomst_Location'], row['Verwerker_Location']]
            if o_pt == 'POINT(nan nan)' or d_pt == 'POINT(nan nan)': continue
            wkt, seq = routing.query(o_pt, d_pt, cur)
            f.write(f'{origin};{destination};{wkt};{seq}\n')
        routing.close_connection(con, cur)


def check_locations_on_postcode(df):
    # import postcode districts
    districts = gpd.read_file("data/areas/Postcodegebied_PC4_RDnew.shp")
    districts = districts[["geometry", "PC4"]]
    districts["centroid"] = districts.geometry.centroid

    # convert locations postcodes to 4-digit
    df["PC4_loc"] = df["postcode"].str[:4]

    # cast district 4-digit postcodes as strings
    districts["PC4"] = districts["PC4"].astype(str)

    # merge locations with districts on geometry
    # point in polygon (with spatial indexes)
    merge_geom = gpd.sjoin(df, districts, how="left", op="within")

    # merge locations with districts on postcode
    # preserve original indices from locations
    merge_code = pd.merge(df, districts, how="left", left_on="PC4_loc", right_on="PC4")
    merge_code.index = df.index  # keep original index

    # check if there is a match on geometry
    # false: no/wrong point from geocoding
    condition = (merge_geom["PC4"] != merge_code["PC4"]) & \
                (merge_code["PC4"].notnull())

    # if no match on geometry, match locations & districts on postcode
    # assign district centroid as location
    df.loc[condition, "geometry"] = merge_code["centroid"]

    # convert geometry into WKT
    df = df.to_crs("epsg:4326")
    df["wkt"] = df.geometry.apply(lambda x: wkt.dumps(x))

    return df["wkt"]


def enrich_data():
    # import ewc classifications
    print('Import EWC classifications...')
    ewc_classifs = {}
    for classif in ['industries', 'agendas']:
        ewc_classifs[classif] = pd.read_csv(f'./data/materials/ewc_{classif}.csv',
                                            low_memory=False,
                                            sep=';')

    # import locations
    print('Import locations...')
    path = '../../../../../media/geofluxus/DATA/national/mapbox_locations.csv'
    geo = pd.read_csv(path, low_memory=False, sep='\t')
    mis_geo = pd.read_csv('afvalprofiel/missing_locations_processed.csv', low_memory=False, sep='\t')
    geo = pd.concat([geo, mis_geo]).reset_index()
    geo.drop_duplicates(inplace=True)

    geo["straat"] = geo["straat"].astype("str")
    geo["straat"] = geo["straat"].apply(clean_address)

    geo["huisnr"] = geo["huisnr"].astype("str")
    geo["huisnr"] = geo["huisnr"].apply(clean_huisnr)

    geo["postcode"] = geo["postcode"].astype("str")
    geo["postcode"] = geo["postcode"].apply(clean_postcode)

    geo["plaats"] = geo["plaats"].astype("str")
    geo["plaats"] = geo["plaats"].apply(clean_address)

    geo["land"] = geo["land"].astype("str")
    geo["land"] = geo["land"].apply(clean_address)

    geo["adres"] = geo["straat"].str.cat(geo[["huisnr", "postcode", "plaats", "land"]], sep=" ")
    geo.drop_duplicates(subset=['adres'], inplace=True)

    geo.loc[geo["lon"] == "None", "lon"] = np.nan
    geo.loc[geo["lat"] == "None", "lat"] = np.nan

    # import routings
    path = '../../../../../media/geofluxus/DATA/national/routings.csv'
    # import routings
    print('Import routings...')
    routings = pd.read_csv(f'../../../../../media/geofluxus/DATA/national/routings.csv', low_memory=False, sep=';')
    new_routes = pd.read_csv(f'afvalprofiel/missing_routings_processed.csv', low_memory=False, sep=';')
    routings = pd.concat([routings, new_routes]).reset_index()
    routings.drop_duplicates(subset=['origin', 'destination'], inplace=True)
    routings['distance'] = gpd.GeoSeries.from_wkt(routings[routings['wkt'].notnull()]['wkt']) \
        .set_crs('epsg:4326') \
        .to_crs('epsg:3857') \
        .length

    # analyse companies
    for company in COMPANIES.keys():
        # import file
        print()
        df = pd.read_csv(f'afvalprofiel/{company}/data/{company}.csv', low_memory=False)
        print(f'Analyse {company}: {len(df)}')
        initial_entries = len(df)

        # add classifications
        for name, classif in ewc_classifs.items():
            df = utils.add_classification(df, classif, name=name,
                                          left_on='EuralCode',
                                          right_on='ewc')

        for role in ['Ontdoener', 'Herkomst', 'Verwerker']:
            columns = list(df.columns)

            # list columns for cleaning
            straat = f"{role}_Straat"
            huisnr = f"{role}_Huisnr"
            postcode = f"{role}_Postcode"
            plaats = f"{role}_Plaats"
            land = f"{role}_Land"

            # clean street name
            df[straat] = df[straat].astype("str")
            df[straat] = df[straat].apply(clean_address)

            # clean house number
            df[huisnr] = df[huisnr].astype("str")
            df[huisnr] = df[huisnr].apply(clean_huisnr)

            # clean postcode
            df[postcode] = df[postcode].astype("str")
            df[postcode] = df[postcode].apply(clean_postcode)

            # clean city name
            df[plaats] = df[plaats].astype("str")
            df[plaats] = df[plaats].apply(clean_address)

            # clean country name
            df[land] = df[land].astype("str")
            df[land] = df[land].apply(clean_address)

            # prepare address for geolocation
            df[f"{role}_Adres"] = df[straat].str.cat(df[[huisnr, postcode, plaats, land]], sep=" ")

            # geolocate
            addresses = pd.merge(df[[f"{role}_Adres", straat, huisnr, postcode, plaats, land]], geo,
                                 how='left', left_on=f"{role}_Adres", right_on="adres", indicator=True)
            addresses[f'{role}_Location'] = addresses.apply(lambda x: to_wkt(x.lon, x.lat), axis=1)
            addresses.index = df.index  # keep original index

            # check for missing geolocations
            unmatched = addresses[addresses._merge == 'left_only']
            if len(unmatched):
                unmatched = unmatched[[straat, huisnr, postcode, plaats, land]]
                unmatched.columns = ['straat', 'huisnr', 'postcode', 'plaats', 'land']
                DATA.setdefault('missing_locations', []).append(unmatched)
                print(f'No locations for {len(unmatched)} flows...')

            # add geolocations to original dataframe
            locations = gpd.GeoDataFrame(addresses, geometry=gpd.points_from_xy(addresses.lon, addresses.lat),
                                         crs="epsg:4326")
            locations = locations.to_crs("epsg:28992")
            df[f'{role}_Location'] = check_locations_on_postcode(locations)
            # df[f'{role}_Location'] = addresses[f'{role}_Location']
            columns.append(f'{role}_Location')
            df = df[columns]

        # add identifiers
        print('Add identifiers to flows...')
        df['Ontdoener_Naam'] = df['Ontdoener'].apply(clean_company_name)
        df['Verwerker_Naam'] = df['Verwerker'].apply(clean_company_name)
        df = add_identifiers(df)

        # add routings
        print('Add routings to flows...')
        df = add_routings(df, routings)

        # export
        df.to_csv(f'afvalprofiel/{company}_processed.csv', index=False)
        print(f'Final entries: {len(df)}')
        final_entries = len(df)
        assert initial_entries == final_entries

        # collect garbage
        del df
        gc.collect()

    # export missing locations & routings
    print()
    for missing, dfs in DATA.items():
        unique = pd.concat(dfs).drop_duplicates()
        unique.to_csv(f'afvalprofiel/{missing}.csv', index=False, sep="\t")
        print(f'{missing}: {len(unique)}')


def to_flowmap(df, extra=[]):
    flows = []
    for idx, row in df.iterrows():
        source_loc = wkt.loads(row['source_loc'])
        target_loc = wkt.loads(row['target_loc'])
        flow = {
            'amount': row['Gewicht_KG'],
            'source': {
                'name': row['source'],
                'lon': source_loc.x,
                'lat': source_loc.y
            },
            'target': {
                'name': row['target'],
                'lon': target_loc.x,
                'lat': target_loc.y
            }
        }
        for ex in extra:
            flow[ex] = row[ex]
        flows.append(flow)

    return flows


def get_flows(df,
              groupby=[], columns={}):
    # extra fields for json
    extra = list(columns.values())

    # groupby columns
    _groupby = groupby.copy()
    _groupby.extend([
        'Ontdoener',
        'Herkomst_Location',
        'Verwerker',
        'Verwerker_Location',
        'Gewicht_KG'
    ])

    columns = {**columns, **{
        'Ontdoener': 'source',
        'Herkomst_Location': 'source_loc',
        'Verwerker': 'target',
        'Verwerker_Location': 'target_loc',
    }}

    # aggregate
    groups = df[_groupby].groupby(_groupby[:-1]).sum().reset_index()
    groups = groups.rename(columns=columns)

    return to_flowmap(groups, extra=extra)


def get_flowmaps():
    YEAR = 2020

    # analyse companies
    for company in COMPANIES.keys():
        # import file
        print()
        df = pd.read_csv(f'afvalprofiel/{company}/data/{company}_processed.csv', low_memory=False)
        df = df[df['MeldPeriodeJAAR'] == 2020]
        print(f'Analyse {company}: {len(df)}')
        df['EuralCode'] = df['EuralCode'].astype(str).str.zfill(6)

        # clean names
        for role in ['Ontdoener', 'Verwerker']:
            df[role] = df[role].apply(clean_company_name)
            df[role] = df[role].str.title()

        # import ewc descriptions
        ewc = pd.read_excel('afvalprofiel/ewc.xlsx')
        ewc['code'] = ewc['code'].astype(str).str.zfill(6)
        ewc['name'] = ewc['name'].str.capitalize()
        ewc.loc[ewc['name'].str.len() > 40, 'name'] = ewc['name'].str[:40] + '...'
        df = pd.merge(df, ewc, how='left',
                      left_on='EuralCode', right_on='code')
        df['ewc'] = df['code'] + ' - ' + df['name']

        DATA[company] = get_flows(df,
                                  groupby=['ewc'],
                                  columns={'ewc': 'ewc'})

    with open(f'afvalprofiel/ewc_flowmap.json', 'w') as fil:
        json.dump(DATA, fil, indent=4)


def create_viz():
    # # import alternatives
    # alternatives = pd.read_csv('afvalprofiel/longlistNH_v4.csv', low_memory=False)
    # alternatives['short_name'] = alternatives['origin'].apply(clean_company_name)
    # reverse_search = {
    #     v: k for k, vals in COMPANIES.items() for v in vals
    # }
    # alternatives['short_name'] = alternatives['short_name'].apply(lambda x: reverse_search.get(x, np.nan))
    # alternatives['ewc_code'] = alternatives['ewc_code'].astype(str).str.zfill(6)

    for company in COMPANIES.keys():
        directory = f'afvalprofiel/{company}/viz'
        if not os.path.exists(directory):
            os.makedirs(directory)

        # import data
        print()
        print(f'Analyse {company}...')
        df = pd.read_csv(f'afvalprofiel/{company}/data/{company}_processed.csv', low_memory=False)
        df['EC_6'] = df['EuralCode'].astype(str).str.zfill(6)
        df['EC_4'] = df['EC_6'].str[:4]
        df['EC_2'] = df['EC_4'].str[:2]

        # # amounts per ewc (last available year) -> barchart
        # year = YEARS[-1]
        # flows = df.copy()
        # flows = flows[flows['MeldPeriodeJAAR'] == year]
        # groupby = [
        #     'EC_2',
        #     'Gewicht_KG'
        # ]
        # groups = flows[groupby].groupby(groupby[:-1]).sum().reset_index()
        # ax = groups.plot.bar(x=groupby[0], y=groupby[1], rot=0)
        # plot.savefig(f'afvalprofiel/{company}/viz/ewc_barchart.png')
        # plot.close()
        #
        # # amounts per ewc & year -> trendline
        # flows = df.copy()
        # groupby = [
        #     'EC_2',
        #     'MeldPeriodeJAAR',
        #     'Gewicht_KG'
        # ]
        # groups = flows[groupby].groupby(groupby[:-1]).sum().reset_index()
        # codes = groups[groupby[0]].drop_duplicates().to_list()
        # directory = f'afvalprofiel/{company}/viz/{company}_ewc_trends'
        # if not os.path.exists(directory):
        #     os.makedirs(directory)
        # for code in codes:
        #     vals = []
        #     for year in YEARS:
        #         search = groups[
        #             (groups[groupby[0]] == code) &
        #             (groups[groupby[1]] == year)
        #         ]
        #         val = search[groupby[-1]].values[0] if len(search) else 0
        #         vals.append(val)
        #     plot.figure().clear()
        #     plot.plot([str(year) for year in YEARS], vals)
        #     plot.savefig(f'{directory}/ewc_{code}_trend.png')
        #     plot.close()
        #
        # # co2 per ewc (last available year) -> barchart
        # year = YEARS[-1]
        # flows = df.copy()
        # flows = flows[flows['MeldPeriodeJAAR'] == year]
        # groupby = [
        #     'EC_2',
        #     'co2'
        # ]
        # groups = flows[groupby].groupby(groupby[:-1]).sum().reset_index()
        # plot.figure().clear()
        # ax = groups.plot.bar(x=groupby[0], y=groupby[1], rot=0)
        # plot.savefig(f'afvalprofiel/{company}/viz/co2_per_ewc_barchart.png')
        #
        # # amount per industry (last available year) -> piechart
        # year = YEARS[-1]
        # flows = df.copy()
        # flows = flows[flows['MeldPeriodeJAAR'] == year]
        # groupby = [
        #     'industries',
        #     'Gewicht_KG'
        # ]
        # groups = flows[groupby].groupby(groupby[:-1]).sum().reset_index()
        # groups.index = groups[groupby[0]]
        # plot.figure().clear()
        # ax = groups.plot.pie(y=groupby[1], rot=0)
        # plot.savefig(f'afvalprofiel/{company}/viz/industries_barchart.png')
        # plot.close()
        #
        # # amount per agenda (last available year) -> piechart
        # year = YEARS[-1]
        # flows = df.copy()
        # flows = flows[flows['MeldPeriodeJAAR'] == year]
        # groupby = [
        #     'agendas',
        #     'Gewicht_KG'
        # ]
        # groups = flows[groupby].groupby(groupby[:-1]).sum().reset_index()
        # groups.index = groups[groupby[0]]
        # plot.figure().clear()
        # ax = groups.plot.pie(y=groupby[1], rot=0)
        # plot.savefig(f'afvalprofiel/{company}/viz/agendas_barchart.png')
        # plot.close()
        #
        # # circular alternatives: amounts per ewc -> barchart
        # # remove duplicates: remove all alt columns & group
        # company_alternatives = alternatives[alternatives['short_name'] == company]
        # print(f'Total alternatives: {len(company_alternatives)}')
        # assert len(company_alternatives) > 0
        # alt_columns = [col for col in company_alternatives.columns if 'alt_' in col]
        # company_alternatives = company_alternatives.drop(columns=alt_columns)
        # company_alternatives = company_alternatives.drop_duplicates()
        # print(f'Total unique alternatives: {len(company_alternatives)}')
        # groupby = [
        #     'ewc_code',
        #     'Gewicht_t'
        # ]
        # groups = company_alternatives[groupby].groupby(groupby[:-1]).sum().reset_index()
        # ax = groups.plot.bar(x=groupby[0], y=groupby[1], rot=0)
        # plot.savefig(f'afvalprofiel/{company}/viz/alternatives_ewc_barchart.png')
        # plot.close()
        #
        # # EWC (6-digit) -> CO2/t per ewc (last available year)
        # year = YEARS[-1]
        # flows = df.copy()
        # flows = flows[flows['MeldPeriodeJAAR'] == year]
        # flows['Gewicht_t'] =  flows['Gewicht_KG'] / 10**3
        # flows['co2_kg'] =  flows['co2'] / 10**3
        # groupby = [
        #     'EC_6',
        #     'Gewicht_t',
        #     'co2_kg'
        # ]
        # groups = flows[groupby].groupby(groupby[0]).sum().reset_index()
        # groups['kgCO2/t'] = groups['co2_kg'] / groups['Gewicht_t']
        # for idx, row in groups.iterrows():
        #     print(f"{row['EC_6']}: {row['kgCO2/t']} kgCO2/t")
        #
        # # CO2 per ewc & year -> trendline (last available year)
        # flows = df.copy()
        # groupby = [
        #     'EC_2',
        #     'MeldPeriodeJAAR',
        #     'co2',
        # ]
        # groups = flows[groupby].groupby(groupby[:-1]).sum().reset_index()
        # codes = groups[groupby[0]].drop_duplicates().to_list()
        # directory = f'afvalprofiel/{company}/viz/{company}_ewc_co2_trends'
        # if not os.path.exists(directory):
        #     os.makedirs(directory)
        # for code in codes:
        #     vals = []
        #     for year in YEARS:
        #         search = groups[
        #             (groups[groupby[0]] == code) &
        #             (groups[groupby[1]] == year)
        #         ]
        #         val = search[groupby[-1]].values[0] if len(search) else 0
        #         vals.append(val)
        #     plot.figure().clear()
        #     plot.plot([str(year) for year in YEARS], vals)
        #     plot.savefig(f'{directory}/ewc_{code}_co2_trend.png')
        #     plot.close()


        # GENERAL INDICATORS
        columns, data = [], []
        year = YEARS[-1]
        flows = df.copy()
        flows = flows[flows['MeldPeriodeJAAR'] == year]

        # total amount of waste (last available year)
        columns.append("Huidige afvalproductie (kg)")
        data.append(flows['Gewicht_KG'].sum())

        # total amount of co2 (last available year)
        columns.append("Huidige CO2 emissies (g)")
        data.append(flows['co2'].sum())

        # largest EWC - 6digit (amount)
        groupby = [
            'EC_6',
            'Gewicht_KG'
        ]
        groups = flows[groupby].groupby(groupby[:-1]).sum().reset_index()
        groups = groups.sort_values(by='Gewicht_KG', ascending=False)
        largest = groups.iloc[0]
        columns.append("Grootste afvalstroom (kg)")
        data.append(largest['EC_6'])

        # largest EWC - 6digit (CO2)
        groupby = [
            'EC_6',
            'co2'
        ]
        groups = flows[groupby].groupby(groupby[:-1]).sum().reset_index()
        groups = groups.sort_values(by='co2', ascending=False)
        largest = groups.iloc[0]
        columns.append("Grootste CO2 afvalstroom (g)")
        data.append(largest['EC_6'])

        # trends
        def compute_trends(df):
            # prepare data
            Y = []
            for year in YEARS:
                amount = df[df['MeldPeriodeJAAR'] == year]
                amount = amount[groupby[-1]].values[0] if len(amount) else 0
                Y.append(amount)
            X = np.array(YEARS).reshape(-1, 1)
            # linear regression
            reg = LinearRegression().fit(X, Y)
            # compute initial & final amount based on model
            Y_initial = reg.predict(np.array(X[0]).reshape(-1, 1))[0]
            Y_final = reg.predict(np.array(X[-1]).reshape(-1, 1))[0]
            # overall change (%)
            change = ((Y_final - Y_initial) / abs(Y_initial)) / len(YEARS) * 100 if Y_initial else np.nan
            return change
        flows = df.copy()

        # Afval stijging sinds 2016
        groupby = [
            'MeldPeriodeJAAR',
            'Gewicht_KG'
        ]
        groups = flows[groupby].groupby(groupby[:-1]).sum().reset_index()
        columns.append("Afval stijging sinds 2016 (%)")
        data.append(compute_trends(groups))

        # CO2 stijging sinds 2016
        groupby = [
            'MeldPeriodeJAAR',
            'co2'
        ]
        groups = flows[groupby].groupby(groupby[:-1]).sum().reset_index()
        columns.append("CO2 stijging sinds 2016 (%)")
        data.append(compute_trends(groups))

        # export results
        results = pd.DataFrame([data], columns=columns)
        results.to_excel(f'afvalprofiel/{company}/data/{company}_indicators.xlsx', index=False, sheet_name='general')

        # EWC INDICATORS (2020)
        columns = {
            'Gewicht_KG': "Huidige afvalproductie (kg)",
            'co2':  "Huidige co2 emissies (g)",
            'Verwerker': "Huidige verwerker",
            'VerwerkingsOmschrijving': "Huiding verwerkingsmethode",
            'kgCO2/t': 'kgCO2/t'
        }
        year = YEARS[-1]
        flows = df.copy()
        flows = flows[flows['MeldPeriodeJAAR'] == year]
        groupby = [
            'Gewicht_KG', # huidige afvalproductie (kg)
            'co2', # huidige co2 emissies (g)
            'Verwerker', # huidige verwerker
            'VerwerkingsOmschrijving', # huiding verwerkingsmethode
            'EC_6'
        ]
        def distinct_join(arg):
            return ', '.join(list(set(arg)))
        groups = flows[groupby].groupby(groupby[-1])\
                .agg({
                    'Gewicht_KG': 'sum',
                    'co2': 'sum',
                    'Verwerker': distinct_join,
                    'VerwerkingsOmschrijving': distinct_join,
                })\
                .reset_index()
        groups['kgCO2/t'] = (groups['co2'] / 10**3) / (groups['Gewicht_KG'] / 10**3)
        groups.rename(columns=columns, inplace=True)

        def append_to_excel(fpath, df, sheet_name):
            with pd.ExcelWriter(fpath, mode="a") as f:
                df.to_excel(f, sheet_name=sheet_name, index=False)
        append_to_excel(f'afvalprofiel/{company}/data/{company}_indicators.xlsx',
                        groups, 'per_ewc')


def get_networkmaps():
    # import network
    with open('./data/network/network.geojson') as f:
        geojson = json.load(f)
        for feat in geojson['features']:
            id = str(feat['properties']['id'])
            distance = feat['properties']['distance']
            NETWORK[id] = (feat['geometry'], distance)

    def get_network(df):
        # distribute along network
        ways = {}
        for idx, flow in df.iterrows():
            seq, amount = flow['seq'], flow['co2'] / (flow['distance'] / 10 ** 3)
            if type(seq) == str:
                seq = [id for id in seq.split('@')]
                if np.isnan(amount): amount = 0
                for id in seq:
                    if id in ways:
                        ways[id] += amount
                    else:
                        ways[id] = amount
        return ways

    for company in COMPANIES.keys():
        YEAR = 2020

        # import company data
        print()
        df = pd.read_csv(f'afvalprofiel/{company}/data/{company}_processed.csv', low_memory=False)
        df = df[df['MeldPeriodeJAAR'] == YEAR]
        print(f'Analyse {company}: {len(df)}')

        # load network
        ways = get_network(df)
        results = []
        for way in NETWORK.items():
            id, props = way
            geometry, distance = props
            id = str(id)
            if id not in ways: ways[id] = 0
            results.append({
                'id': id,
                'geometry': geometry,
                'amount': round(ways[id] * (distance / 10 ** 3) / 10 ** 3, 2),  # grams -> tn
                'period': f'{YEAR}'
            })

        DATA[company] = results

    with open(f'afvalprofiel/networkmap.json', 'w') as fil:
        json.dump(DATA, fil, indent=4)


if __name__ == "__main__":
    df = pd.read_excel('afvalprofiel/shortlist_alternative_names.v1.0.xlsx')
    for index, row in df.iterrows():
        COMPANIES.setdefault(row['Shortname'], []).append(row['Origin name'])
    print(json.dumps(COMPANIES, indent=4))

    # get_company_data()
    # enrich_data()
    # get_flowmaps()
    # get_networkmaps()
    # compute_routings()
    # create_viz()

    # alternatives = pd.read_excel('afvalprofiel/longlistNH_v4.0_hashtag.xlsx', sheet_name='Result 1')
    # cols = [
    #     'Gewicht_t',
    #     'sum_vrachten',
    #     'sum_months'
    # ]
    # alternatives = alternatives.sort_values(by='Gewicht_t', ascending=False)
    # alternatives['desc_num'] = alternatives['string_agg'].str.split('#').str.len()
    # for col in cols:
    #     alternatives[col] = alternatives[col] / alternatives['desc_num']
    # alternatives[cols].to_excel('longlist_corrected.xlsx', index=False)