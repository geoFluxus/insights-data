import pandas as pd
import numpy as np


def format_output_table(df, save_path=None):

    label_order = {
        'Hergebruik':0,
        'Recycle':1,
        'Overige':2,
        'Opslag':3,
        'Totaal':4
    }
    df = df.groupby(['VerwerkingsGroep', 'VerwerkingsPlaats'])['Gewicht_KG'].sum().reset_index()
    df = df.pivot(index='VerwerkingsGroep', values='Gewicht_KG', columns='VerwerkingsPlaats')
    cols =  df.columns.tolist()

    df.columns = pd.MultiIndex.from_tuples([('Verwerkings Plaats', x) for x in cols])
    df[('Verwerkings Plaats', 'Totaal')] = df.sum(axis=1)
    df.loc['Totaal'] = df.sum(numeric_only=True, axis=0)
    df.sort_index(key=lambda x: x.map(label_order), inplace=True)
    if save_path is not None:
        df.to_excel(save_path)

    return df

def twente_processing():
    # Import files
    twente_municipalities = [
        'HELLENDOORN',
        'TWENTERAND',
        'WIERDEN',
        'ALMELO',
        'BORNE',
        'HOF VAN TWENTE',
        'RIJSSEN',
        'TUBBERGEN',
        'DINKELLAND',
        'OLDENZAAL',
        'LOSSER',
        'ENSCHEDE',
        'HAAKSBERGEN',
        'HENGELO OV'
    ]
    description_folder = r"C:\Users\Arthur\geoFluxus Dropbox\geoFluxus\Database_LockedFiles\DATA\descriptions"
    ontology_folder = r"C:\Users\Arthur\geoFluxus Dropbox\geoFluxus\Database_LockedFiles\DATA\ontology"
    waste_categories_processes = pd.read_excel(description_folder+"\ewc_minimum_proc.xlsx")
    waste_processing_groups = pd.read_excel(description_folder+r"\rd_lma_codes.xlsx", sheet_name='rd_lma_hierarchy')
    lma = pd.read_csv(r"C:\Users\Arthur\geoFluxus Dropbox\geoFluxus\Monitors\GemeenteEnschede\LMA\processed\ontvangst_enschede_2023_full.csv")
    lma = lma[lma['Herkomst_Plaats']== 'ENSCHEDE']
    print(lma['Gewicht_KG'].sum())
    ewc_agendas = pd.read_csv(ontology_folder + r"\ewc_agendas.csv", sep=';')
    ewc_materials = pd.read_csv(ontology_folder + r"\ewc_materials.csv", sep=';')

    #rename columns
    waste_processing_groups = waste_processing_groups[['processing_code', 'Monitor group_LAP']]
    waste_processing_groups.rename(columns={'processing_code': 'VerwerkingsmethodeCode',
                                            'Monitor group_LAP':'VerwerkingsGroep'}, inplace=True)
    waste_categories_processes['EuralCode'] = waste_categories_processes['EuralCode'].astype(int)
    waste_categories_processes = waste_categories_processes[['EuralCode', 'SP_naam']].drop_duplicates()
    waste_categories_processes['SP_naam'] = waste_categories_processes.groupby('EuralCode')['SP_naam'].transform(lambda x: ','.join(x))
    waste_categories_processes = waste_categories_processes[['EuralCode', 'SP_naam']].drop_duplicates()
    print(waste_categories_processes['EuralCode'].value_counts())

    ewc_agendas.rename(columns={'ewc':'EuralCode'}, inplace=True)
    ewc_materials.rename(columns={'ewc':'EuralCode'}, inplace=True)

    #print(ewc_materials[ewc_materials['materials'].str.contains('MetaalMateriaal')])
    #Group data by euralcode and processing method, sum KG waste
    lma['VerwerkingsPlaats'] = lma['Verwerker_Plaats'].apply(lambda x: 'Binnen Twente' if x in twente_municipalities else 'Buiten Twente')
    lma = lma.groupby(['EuralCode', 'VerwerkingsmethodeCode', 'VerwerkingsPlaats'])['Gewicht_KG'].sum().reset_index()



    # Add waste type names (from LAP categorisation)
    lma = pd.merge(lma, waste_categories_processes, on='EuralCode', how='left', validate='many_to_one')

    # Add type of processing (recycling, storage, or other)
    lma = pd.merge(lma, waste_processing_groups, on='VerwerkingsmethodeCode', how='left')
    lma.loc[lma['VerwerkingsmethodeCode'].isin(['A01', 'A02']), 'VerwerkingsGroep'] = 'Opslag'

    #Add material chains
    lma = pd.merge(lma, ewc_materials, on='EuralCode', how='left')

    #Add transitieagendas
    lma = pd.merge(lma, ewc_agendas, on='EuralCode', how='left')


    lma.to_excel("C:\\Users\\Arthur\\Downloads\\Enschede_lma_per_material_new.xlsx", index=False)

    # # Textiles retrieved form the descriptions -> ewc min proc file
    # # Metals taken from ontology -> ewc_materials
    # # Bouw taken from ontology -> ewc_agenda's
    textiles = ['Gescheiden ingezameld/afgegeven textiel (inclusief schoeisel)', 'Gescheiden ingezameld/afgegeven textiel (inclusief schoeisel),Overige recyclebare monostromen (matrassen, steenwol, tapijt, kunstgras en luiers/incontinentiemateriaal)']
    metals = ['Metalen', 'Metalen met aanhangende olie of emulsie', 'Metalen,Shredderafval', 'Metalen,Metalen met aanhangende olie of emulsie']
    electronics = ['Afgedankte elektrische en elektronische apparatuur']#,'Batterijen en accu’s']
    #
    lma_tex = lma[lma['SP_naam'].isin(textiles)]
    lma_metals = lma[lma['SP_naam'].isin(metals)]
    lma_electronics = lma[lma['SP_naam'].isin(electronics)]
    lma_boulding = lma[(lma['EuralCode'].astype(str).str[:2] == '17') & (lma['EuralCode'].astype(str).str.len() == 6)]
    lma_dict = {
        'Textiel':lma_tex,
        'Metalen':lma_metals,
        'Elektronica':lma_electronics,
        'Bouw':lma_boulding
    }
    with pd.ExcelWriter("C:\\Users\\Arthur\\Downloads\\Enschede_indicator_tabellen.xlsx") as writer:
        for i in lma_dict.keys():
            df = format_output_table(lma_dict[i])
            print(df)
            df.to_excel(writer, sheet_name=i)

def test_inclusion(x):
    road_keywords = ['afsalt', 'teerhoudend', '1703201', 'afalt', '170301', 'dakbedekking', 'bitumen', 'asfalt',
                     'bitumineuz', 'frees', 'teer', 'schollen', 'bitumineuze stoffen', 'granulaat', 'bitumineus',
                     'freesmateriaal', 'teerafvallen', 'tag', 'fres', 'bitumenemulsie', 'bitumen emulsie',
                     'bitum.m']
    included = 'Nee'
    for i in road_keywords:
        if i in x:
            if i == 'teer':
                if 'sorteer' in x or 'composteer' in x or 'gerelateer' in x:
                    continue
                else:
                    included = 'Ja'
                    break
            else:
                included = 'Ja'
                break
    return included
if __name__ == '__main__':

    description_folder = r"C:\Users\Arthur\geoFluxus Dropbox\geoFluxus\Database_LockedFiles\DATA\descriptions"
    ontology_folder = r"C:\Users\Arthur\geoFluxus Dropbox\geoFluxus\Database_LockedFiles\DATA\ontology"
    waste_categories_processes = pd.read_excel(description_folder + "\ewc_minimum_proc.xlsx")

    improvement_potential = pd.read_excel("C:\\Users\\Arthur\\PycharmProjects\\insights-data\\json Schagen\\afval_verbeteringspotentieel.xlsx")
    waste_processing_groups = pd.read_excel(description_folder + r"\rd_lma_codes.xlsx", sheet_name='rd_lma_hierarchy')
    waste_tas = pd.read_csv(ontology_folder+'\ewc_agendas.csv', delimiter=';')

    waste_tas.rename(columns={'ewc':'EuralCode'}, inplace=True)
    improvement_potential.rename(columns={'eural_code': 'EuralCode'}, inplace=True)
    lma = pd.read_csv(
        r"C:\Users\Arthur\geoFluxus Dropbox\geoFluxus\Monitors\GemeenteSchagen\LMA\processed\ontvangst_schagen_2023_full.csv")
    lma = lma[lma['Herkomst_Plaats'] == 'SCHAGEN']

    lma = pd.merge(lma, waste_tas, on='EuralCode', how='left', validate='many_to_one')
    improvement_potential = pd.merge(improvement_potential, waste_tas, on='EuralCode', how='left', validate='many_to_one')

    lma['Wegafval'] = lma['BenamingAfval'].apply(test_inclusion)
    print(lma['Wegafval'])
    wegafval = lma[lma['Wegafval'] == 'Ja'].groupby(['VerwerkingsOmschrijving'])['Gewicht_KG'].sum().reset_index()
    wegafval.to_excel('C:\\Users\\Arthur\\Downloads\\Schagen_wegafval_groep.xlsx')

    sector_potential = improvement_potential.groupby(['benchmark_group', 'benchmark_group_alt', 'agendas'])['amount_kg'].sum().reset_index()
    # mask[i,j] is True for the lower triangle (including diagonal)
    print(sector_potential['amount_kg'].sum())
    lower_tri_mask = np.tril(np.ones((9,9), dtype=bool))
    lower_tri_mask[8,:8] = False

    index = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I']
    renamer = {
        'A': 'A Direct hoogwaardig inzetten',
        'B': 'B Indirect hoogwaardig inzetten',
        'C': 'C Voorbereiding voor recycling',
        'D': 'D Microbiologische verwerking',
        'E': 'E Grondreiniging',
        'F': 'F Verbranding met opbrengst',
        'G': 'G Verbranden',
        'H': 'H Storten',
        'I': 'I Opslag'
    }
    cols = pd.MultiIndex.from_tuples([('Best beschikbare methode', x) for x in index])
    # create an array that’s 0 where mask is True, NaN otherwise
    data = np.where(lower_tri_mask, 0, np.nan)

    empty_matrix = pd.DataFrame(data, columns=cols, index=index)
    empty_matrix.index.name = 'Huidige verwerking'
    results = {str(i):empty_matrix.copy() for i in sector_potential['agendas'].unique() if not '&' in i}

    for i in sector_potential['agendas'].unique():
        sector_data = sector_potential[sector_potential['agendas'] == i]
        mat = empty_matrix.copy()
        for ind, row in sector_data.iterrows():
            mat.loc[row['benchmark_group'], ('Best beschikbare methode', row['benchmark_group_alt'])] = row['amount_kg']
        names = str(i).split('&')
        factor = 1/len(names)
        for n in names:

            if n in results:
                results[n] += mat * factor
            else:
                results[n] = mat * factor
    total = 0
    with pd.ExcelWriter('C:\\Users\\Arthur\\geoFluxus Dropbox\\geoFluxus\\Monitors\\Extra datalevering klanten\\Gemeente Schagen\\Schagen afvalverbetering per sector.xlsx',  engine='xlsxwriter') as writer:
        for res in results:
            name = res if not 'TransitieAgenda' in res else res.split('TransitieAgenda')[0]
            results[res].rename(index=renamer, inplace=True)
            results[res].to_excel(writer, sheet_name=name)
            sheet = writer.sheets[name]
            sheet.set_column('A:A', 30)
            total += results[res].sum().sum()
    print(total)