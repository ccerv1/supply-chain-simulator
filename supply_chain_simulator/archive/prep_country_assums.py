import json
import pandas as pd
from pathlib import Path
import numpy as np

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
LOCAL_DATA_DIR = DATA_DIR / '_local'
SIMULATION_DATA_PATH = DATA_DIR / 'country_assums.csv'
COUNTRY_CODE_MAPPING = json.load(open(DATA_DIR / 'country_codes.json'))

EU_COUNTRIES = [
    'Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Cyprus', 'Czechia', 'Denmark',
    'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary', 'Ireland',
    'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Netherlands', 'Poland',
    'Portugal', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden'
]
    
def load_data():
    comtrade_path = LOCAL_DATA_DIR / 'comtrade_2019.csv'
    geo_path = LOCAL_DATA_DIR / 'jebena_geo_map_dataset.csv'
    
    comtrade = pd.read_csv(comtrade_path, index_col=0)
    geo = pd.read_csv(geo_path, thousands=',')
    geo.columns = ['ssu_code', 'label', 'value', 'variable']
    return comtrade, geo

def create_country_mappings():
    short_to_full = {}
    full_to_short = {}
    for country_data in COUNTRY_CODE_MAPPING.values():
        short_to_full[country_data['short']] = country_data['full']
        full_to_short[country_data['full']] = country_data['short']
    return short_to_full, full_to_short

SHORT_TO_FULL, FULL_TO_SHORT = create_country_mappings()

def process_comtrade_data(comtrade):
    # Map ISO codes to full country names
    exporters = comtrade[['ISO', 'Country']].drop_duplicates()
    exporters['Country'] = exporters['ISO'].map(SHORT_TO_FULL)
    exporters = exporters.set_index('ISO')['Country'].to_dict()
    
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
    # Map the two-letter codes to full country names using the JSON data
    geo['country'] = geo['country_code'].apply(
        lambda x: COUNTRY_CODE_MAPPING.get(x, {}).get('full', 'Other')
    )
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

def add_supply_chain_assumptions(comtrade_data, production_data):
    """Add assumptions for number of exporters and middlemen based on country size"""
    enhanced_data = {}
    
    for country in production_data:
        country_data = {}
        country_data['num_farmers'] = production_data[country].get('num_farmers', 0)
        country_data['total_production'] = production_data[country].get('est_production', 0)
        country_data['exports_to_eu'] = comtrade_data.get(country, {}).get('EU', 0)
        country_data['exports_to_other'] = comtrade_data.get(country, {}).get('Other', 0)

        if country_data['total_production'] > 0:
            # Base number of 20 exporters, scaling up with production
            # Cap at 200 exporters for very large producers
            country_data['num_exporters'] = min(
                20 + int(np.sqrt(country_data['total_production'] / 1e6 * 1.5)),
                200
            )
        else:
            country_data['num_exporters'] = 0

        # Add middleman assumptions - scales with number of farmers
        if country_data['num_farmers'] > 0:
            # 100 middlemen for small countries, scaling up to 5000 middlemen for very large producer countries
            # Cap at 5000 middlemen for very large producer countries
            country_data['num_middlemen'] = min(
                100 + int(country_data['num_farmers'] / 500),
                5000
            )
        else:
            country_data['num_middlemen'] = 0

        enhanced_data[country] = country_data
        
    return enhanced_data

def export_data(enhanced_data):
    """Export the combined data to CSV"""
    combined_data = pd.DataFrame.from_dict(enhanced_data, orient='index')
    combined_data.index.name = 'country'
    combined_data.to_csv(SIMULATION_DATA_PATH)

def main():
    comtrade, geo = load_data()
    comtrade_data, exporters = process_comtrade_data(comtrade)
    production_data = process_geo_data(geo, exporters)
    enhanced_data = add_supply_chain_assumptions(comtrade_data, production_data)
    export_data(enhanced_data)

if __name__ == "__main__":
    main()