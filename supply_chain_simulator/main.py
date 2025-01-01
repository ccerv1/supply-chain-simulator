from initialize import initialize_country
from models import Country, Farmer, Middleman, Exporter, Geography
from trade_sim import simulate_next_year
import pandas as pd
from pathlib import Path
import sqlite3

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
DB_PATH = DATA_DIR / 'test_supply_chain.db'


def main():
    # Create a test country
    test_country = Country(id="CR")

    # Initialize country
    initialize_country(test_country, DB_PATH)

    # Verify the initialization
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # Check geographies
        geo_df = pd.read_sql_query("SELECT * FROM geographies", conn)
        print("\nGeographies created:")
        print(f"Total geographies: {len(geo_df)}")
        print(geo_df.tail())

        country_df = pd.read_sql_query("SELECT * FROM countries", conn)
        print("\nCountries created:")
        print(f"Total countries: {len(country_df)}")
        print(country_df.head())


        # Check farmers
        farmer_df = pd.read_sql_query("SELECT * FROM farmers", conn)
        print("\nFarmers created:")
        print(f"Total farmers: {len(farmer_df)}")
        print(f"Total plots: {farmer_df['num_plots'].sum()}")
        print(f"Total production: {farmer_df['production_amount'].sum()}")
        print(farmer_df.tail())

        # Check middlemen
        middlemen_df = pd.read_sql_query("SELECT * FROM middlemen", conn)
        print("\nMiddlemen created:")
        print(f"Total middlemen: {len(middlemen_df)}")
        print("Competitiveness stats:")
        print(middlemen_df['competitiveness'].describe())
        print(middlemen_df.head())

        # Check exporters
        exporters_df = pd.read_sql_query("SELECT * FROM exporters", conn)
        print("\nExporters created:")
        print(f"Total exporters: {len(exporters_df)}")
        print("Competitiveness stats:")
        print(exporters_df['competitiveness'].describe())
        print(exporters_df.head())

        # Check trading relationships
        trading_df = pd.read_sql_query("""
            SELECT 
                tr.country,
                COUNT(DISTINCT tr.farmer_id) as num_farmers,
                COUNT(DISTINCT tr.middleman_id) as num_middlemen,
                COUNT(DISTINCT tr.exporter_id) as num_exporters,
                SUM(tr.amount_kg) as total_volume,
                SUM(CASE WHEN tr.sold_to_eu THEN tr.amount_kg ELSE 0 END) as eu_volume,
                AVG(CASE WHEN tr.sold_to_eu THEN 1 ELSE 0 END) as eu_ratio
            FROM trading_relationships tr
            GROUP BY tr.country
        """, conn)
        print("\nTrading relationships summary:")
        print(f"Total volume traded: {trading_df['total_volume'].iloc[0]:,} kg")
        print(f"Volume exported to EU: {trading_df['eu_volume'].iloc[0]:,} kg")
        print(f"Percentage to EU: {(trading_df['eu_volume'].iloc[0] / trading_df['total_volume'].iloc[0] * 100):.1f}%")
        
        # Check distribution of relationships
        relationship_stats = pd.read_sql_query("""
            WITH FarmerStats AS (
                SELECT 
                    farmer_id,
                    COUNT(DISTINCT middleman_id) as num_middlemen
                FROM trading_relationships
                GROUP BY farmer_id
            ),
            MiddlemanStats AS (
                SELECT 
                    middleman_id,
                    COUNT(DISTINCT exporter_id) as num_exporters
                FROM trading_relationships
                GROUP BY middleman_id
            )
            SELECT 
                'Farmers' as actor_type,
                AVG(num_middlemen) as avg_relationships,
                MIN(num_middlemen) as min_relationships,
                MAX(num_middlemen) as max_relationships
            FROM FarmerStats
            UNION ALL
            SELECT 
                'Middlemen' as actor_type,
                AVG(num_exporters) as avg_relationships,
                MIN(num_exporters) as min_relationships,
                MAX(num_exporters) as max_relationships
            FROM MiddlemanStats
        """, conn)
        
        print("\nRelationship distribution:")
        for _, row in relationship_stats.iterrows():
            print(f"{row['actor_type']}:")
            print(f"  Average buyers per actor: {row['avg_relationships']:.1f}")
            print(f"  Range: {row['min_relationships']} to {row['max_relationships']} buyers")

        # First, get geographies (we need these to link to farmers)
        geographies = pd.read_sql_query("SELECT * FROM geographies", conn).to_dict('records')
        geography_map = {
            g['id']: Geography(
                id=g['id'],
                name=g['name'],
                centroid=g['centroid'],
                producing_area_name=g['producing_area_name'],
                num_farmers=g['num_farmers'],
                total_production_kg=g['total_production_kg'],
                primary_crop=g['primary_crop'],
                country=test_country
            ) 
            for g in geographies
        }
        
        # Get farmers and link them to their geography objects
        farmers = pd.read_sql_query("SELECT * FROM farmers", conn).to_dict('records')
        farmers = [
            Farmer(
                id=f['id'],
                geography=geography_map[f['geography_id']],
                num_plots=f['num_plots'],
                production_amount=f['production_amount'],
                middleman_loyalty=f['middleman_loyalty']
            ) 
            for f in farmers
        ]
        
        # Get middlemen and exporters (these can be created directly)
        middlemen = pd.read_sql_query("SELECT * FROM middlemen", conn).to_dict('records')
        middlemen = [
            Middleman(
                id=m['id'],
                competitiveness=m['competitiveness'],
                farmer_loyalty=m['farmer_loyalty'],
                exporter_loyalty=m['exporter_loyalty']
            ) 
            for m in middlemen
        ]
        
        exporters = pd.read_sql_query("SELECT * FROM exporters", conn).to_dict('records')
        exporters = [
            Exporter(
                id=e['id'],
                competitiveness=e['competitiveness'],
                eu_preference=e['eu_preference'],
                middleman_loyalty=e['middleman_loyalty']
            ) 
            for e in exporters
        ]

        # Get year 0 relationships
        relationships = pd.read_sql_query("SELECT * FROM trading_relationships", conn).to_dict('records')

        # Simulate years 1 and 2
        for year in [1, 2]:
            print(f"\nSimulating year {year}...")
            new_relationships = simulate_next_year(
                relationships,
                test_country,
                farmers,
                middlemen,
                exporters,
                geographies,
                year
            )
            
            # Insert new relationships into database
            relationship_df = pd.DataFrame(new_relationships)
            relationship_df.to_sql('trading_relationships', conn, if_exists='append', index=False)
            
            # Update relationships for next iteration
            relationships = new_relationships
            
            # Print summary for this year
            year_summary = pd.read_sql_query(f"""
                SELECT 
                    year,
                    COUNT(DISTINCT farmer_id) as num_farmers,
                    COUNT(DISTINCT middleman_id) as num_middlemen,
                    COUNT(DISTINCT exporter_id) as num_exporters,
                    SUM(amount_kg) as total_volume,
                    SUM(CASE WHEN sold_to_eu THEN amount_kg ELSE 0 END) as eu_volume
                FROM trading_relationships
                WHERE year = {year}
                GROUP BY year
            """, conn)
            
            print(f"Year {year} summary:")
            print(f"Total volume traded: {year_summary['total_volume'].iloc[0]:,} kg")
            print(f"Volume to EU: {year_summary['eu_volume'].iloc[0]:,} kg")
            print(f"Active farmers: {year_summary['num_farmers'].iloc[0]}")
            print(f"Active middlemen: {year_summary['num_middlemen'].iloc[0]}")
            print(f"Active exporters: {year_summary['num_exporters'].iloc[0]}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()