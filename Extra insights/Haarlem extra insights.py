import pandas as pd
import os
# folder = 'C:\\Users\\Arthur\\geoFluxus Dropbox\\geoFluxus\\Monitors\\GemeenteHaarlem\\LMA\\'
# for i in os.listdir(folder + 'processed_old'):
#     df = pd.read_csv(folder + 'processed_old\\' + i, dtype={'EuralCode': float})
#     or_len = len(df)
#     df = df[~((df['Verwerker'].isin(['SPAARNLANDEN', 'SPAARNELANDEN']))  & (df["EuralCode"] == 200301) & (df['VerwerkingsmethodeCode'] == 'A02'))]
#     print(f'removed {or_len - len(df)} rows out of {or_len} for {i}')
#     #df.to_csv(folder + 'processed\\' + i, index=False)
group_names = ['Productie goederen', 'Dienstverlening bedrijven', 'Overheid', 'Consumptie huishoudens',
               'Investeringen vaste activa', 'Verandering voorraden']
new_names = {
    'Bedrijven': ['Productie goederen', 'Dienstverlening bedrijven'],
    'Huishoudens': ['Consumptie huishoudens'],
    'Overheden': ['Overheid'],
    'Niet in te delen': ['Investeringen vaste activa', 'Verandering voorraden']
}

#CREATE GEBRUIKERSGROEPEN SPLIT
streams = ['Invoer_nationaal', 'Invoer_internationaal', 'Aanbod_eigen_regio']
data = pd.read_csv(r"C:\Users\Arthur\geoFluxus Dropbox\geoFluxus\Database_LockedFiles\DATA\monitor_data\data\CBS\Tabel Regionale stromen 2015-2023 Coropplus.csv")
data = data[data['Regionaam']=='Agglomeratie Haarlem']
data = data[data['Gebruiksgroep_naam'].isin(group_names)]
geb_data = data.groupby(['Stroom', 'Gebruiksgroep_naam', 'Jaar'])['Brutogew'].sum().reset_index()

dat_piv = pd.pivot(geb_data, columns=['Stroom', 'Gebruiksgroep_naam'], index='Jaar', values='Brutogew')
print(dat_piv)
small_original = pd.DataFrame(0, columns=pd.MultiIndex.from_product([['Invoer en aanbod'], group_names]), index=dat_piv.index)
streams_table = pd.DataFrame(0, columns=pd.MultiIndex.from_product([streams, new_names.keys()]), index=dat_piv.index)
streams_table_small = pd.DataFrame(0, columns=pd.MultiIndex.from_product([['Invoer en aanbod'], new_names.keys()]), index=dat_piv.index)
for i in new_names.keys():
    for j in streams:
        for t in new_names[i]:
            streams_table[(j,i)] += dat_piv[(j,t)]

        streams_table_small[('Invoer en aanbod', i)] += streams_table[(j,i)]

for i in group_names:
    for j in streams:
        small_original[('Invoer en aanbod',i)] += dat_piv[(j,i)]

transition_agendas = pd.read_excel(r"C:\Users\Arthur\geoFluxus Dropbox\geoFluxus\Database_LockedFiles\DATA\monitor_data\data\geoFluxus\CBS_names.xlsx", sheet_name='CBS_code_merger')[['Goederengroep_naam', 'TA']]
data = pd.merge(data,transition_agendas, how='left', on='Goederengroep_naam')

with pd.ExcelWriter('C:\\Users\\Arthur\\geoFluxus Dropbox\\geoFluxus\\Monitors\\Extra datalevering klanten\\Gemeente Haarlem\\Juni 2025\\Invoer en aanbod per TA en gebruiker.xlsx') as writer:
    streams_table_small.to_excel(writer, sheet_name='Gebruikersgroepen')
    small_original.to_excel(writer, sheet_name='Gebruikersgroepen CBS')
    for year in range(2015, 2024):
        ta_users = data[data['Jaar'] == year]
        ta_users = ta_users.groupby(['TA', 'Gebruiksgroep_naam'])[('Bru'
                                                                   'togew')].sum().reset_index()
        ta_users = pd.pivot(ta_users, columns='TA', index = 'Gebruiksgroep_naam', values='Brutogew')
        ta_users['Kunststoffen'] = 0
        cols = list(ta_users.columns)
        categories = []
        for i in range(len(cols)):
            if len(cols[i].split(', ')) > 1:
                for j in cols[i].split(', '):
                    ta_users[j] += 0.5 * ta_users[cols[i]]
            else:
                categories.append(cols[i])
        ta_users = ta_users[categories]
        ta_users.rename(columns={'Bouw': 'Bouwmaterialen', 'Non-specifiek': 'Overig'}, inplace=True)
        ta_users = ta_users[['Biomassa en voedsel',
                             'Kunststoffen',
                             'Bouwmaterialen',
                             'Consumptiegoederen',
                             'Maakindustrie',
                             'Overig']]
        ta_short_names = pd.DataFrame(0.0, index=list(new_names.keys()), columns = ta_users.columns)
        ta_short_names.index.name = 'Gebruikersgroep'
        for i in new_names.keys():
            for k in new_names[i]:
                ta_short_names.loc[i] += ta_users.loc[k]
        ta_short_names.columns = pd.MultiIndex.from_product([['Invoer en aanbod'], ta_short_names.columns])
        ta_short_names.to_excel(writer, sheet_name=str(year))


def save_file():
    file = 'C:\\Users\\Arthur\\Downloads\\Haarlem_TA.xlsx'
    tabs = ['DMI', 'DMC', 'CO2', 'MKI']
    with pd.ExcelWriter('C:\\Users\\Arthur\\geoFluxus Dropbox\\geoFluxus\\Monitors\\Extra datalevering klanten\\Gemeente Haarlem\\Juni 2025\\Transitie agendas.xlsx') as writer:
        for i in range(len(tabs)):
            df = pd.read_excel(file, sheet_name=tabs[i])
            val = 'kt' if tabs[i] != 'MKI' else 'mln euro'
            piv = df.pivot(index='Jaar', columns='TA', values=val)
            print(piv)
            piv.to_excel(writer, sheet_name=tabs[i])

save_file()