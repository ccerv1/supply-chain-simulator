import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
LOCAL_DATA_DIR = DATA_DIR / '_local'
SIMULATION_DATA_PATH = DATA_DIR / 'simulation_data.csv'

EU_COUNTRIES = [
    'Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Cyprus', 'Czechia', 'Denmark',
    'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary', 'Ireland',
    'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Netherlands', 'Poland',
    'Portugal', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden'
]

COUNTRY_CODE_MAPPING = {
    'au': 'AUS', 'bi': 'BDI', 'bo': 'BOL', 'br': 'BRA', 'cd': 'COD', 'ci': 'CIV',
    'cn': 'CHN', 'co': 'COL', 'cr': 'CRI', 'ec': 'ECU', 'et': 'ETH', 'gt': 'GTM',
    'hn': 'HND', 'id': 'IDN', 'in': 'IND', 'ke': 'KEN', 'la': 'LAO', 'mx': 'MEX',
    'my': 'MYS', 'ni': 'NIC', 'pa': 'PAN', 'pe': 'PER', 'pg': 'PNG', 'rw': 'RWA',
    'sv': 'SLV', 'th': 'THA', 'tl': 'TLS', 'tz': 'TZA', 'ug': 'UGA', 'vn': 'VNM',
    'ye': 'YEM', 'zm': 'ZMB'
}

def load_data():
    comtrade_path = LOCAL_DATA_DIR / 'comtrade_2019.csv'
    geo_path = LOCAL_DATA_DIR / 'jebena_geo_map_dataset.csv'
    
    comtrade = pd.read_csv(comtrade_path, index_col=0)
    geo = pd.read_csv(geo_path, thousands=',')
    geo.columns = ['ssu_code', 'label', 'value', 'variable']
    return comtrade, geo

def process_comtrade_data(comtrade):
    exporters = comtrade[['ISO', 'Country']].drop_duplicates().set_index('ISO')['Country'].to_dict()
    comtrade['EU'] = comtrade['ImportCountry'].apply(lambda x: 'EU' if x in EU_COUNTRIES else 'Other')
    comtrade_data = comtrade.pivot_table(
        index='Country',
        columns='EU',
        values='Weight',
        aggfunc='sum'
    ).T.to_dict()
    return comtrade_data, exporters

def process_geo_data(geo, exporters):
    geo['country_code'] = geo['ssu_code'].apply(lambda x: x[:2])
    geo['country'] = geo['country_code'].apply(lambda x: exporters.get(COUNTRY_CODE_MAPPING.get(x), 'Other'))
    countries = geo.pivot_table(
        index='country',
        columns='variable',
        values='value',
        aggfunc='sum'
    )
    countries['num_farmers'] = countries['estimated_arabica_farmer_population'] + countries['estimated_robusta_farmer_population']
    countries['est_production'] = countries['estimated_arabica_production_in_kg'] + countries['estimated_robusta_production_in_kg']
    production_data = countries[['num_farmers', 'est_production']].T.to_dict()
    return production_data

def export_data(comtrade_data, production_data):
    comtrade_df = pd.DataFrame.from_dict(comtrade_data, orient='index')
    production_df = pd.DataFrame.from_dict(production_data, orient='index')
    combined_data = comtrade_df.join(production_df, how='outer')
    combined_data = combined_data[['num_farmers', 'est_production', 'EU', 'Other']]
    combined_data.columns = [
        'Total Farmers', 
        'Total Production', 
        'Exports to EU', 
        'Exports to Other Destinations'
    ]
    combined_data.index.name = 'Country'
    combined_data.to_csv(SIMULATION_DATA_PATH)

def main():
    comtrade, geo = load_data()
    comtrade_data, exporters = process_comtrade_data(comtrade)
    production_data = process_geo_data(geo, exporters)
    export_data(comtrade_data, production_data)

if __name__ == "__main__":
    main()