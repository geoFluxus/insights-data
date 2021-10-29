# import pandas as pd
#
# path = f'../../../../../media/geofluxus/DATA/national/utrecht/raw'
# df = pd.read_csv(f'{path}/afgifte_utrecht_2016_2019_raw.csv', low_memory=False)
# for year in range(2016, 2020):
#     print(f'Year: {year}')
#     df_new = df[df['MeldPeriodeJAAR'] == year]
#     df_new.to_csv(f'{path}/afgifte_utrecht_{year}_raw.csv', index=False)