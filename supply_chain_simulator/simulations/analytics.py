"""
Supply Chain Analytics

This module provides analysis and visualization capabilities for the coffee supply chain simulation.
"""

from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

from database.manager import DatabaseManager
from database.registries import TradingRegistry, GeographyRegistry
from supply_chain_simulator.models.trade_flow import TradingRelationship

class SupplyChainAnalytics:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.trading_registry = TradingRegistry(db_manager)
        self.geography_registry = GeographyRegistry(db_manager)
        
    def generate_year_summary(self, year: int, country_id: str) -> Dict:
        """Generate summary statistics for a given year."""
        summary = self.trading_registry.get_year_summary(year, country_id)
        relationship_stats = self.trading_registry.get_relationship_stats(year, country_id)
        
        return {
            'year': year,
            'summary': summary,
            'relationship_stats': relationship_stats
        }

    def analyze_trade_evolution(self, country_id: str):
        """Analyze the evolution of trading relationships over time."""
        # Get all relationships and filter by country
        all_relationships = []
        years = self.trading_registry.get_all_years()
        
        if not years:
            # Return empty DataFrame with expected columns if no data exists
            return pd.DataFrame(columns=[
                'year', 'total_volume', 'eu_volume', 'eu_ratio',
                'num_farmers', 'num_middlemen', 'num_exporters',
                'avg_middlemen_per_farmer', 'avg_exporters_per_middleman',
                'avg_volume_per_farmer'
            ])
        
        for year in years:
            year_relationships = self.trading_registry.get_by_year(year)
            # Filter relationships for the specific country
            country_relationships = [
                rel for rel in year_relationships 
                if rel.farmer.country_id == country_id
            ]
            all_relationships.extend(country_relationships)
        
        # Convert to DataFrame
        df = pd.DataFrame([r.to_dict() for r in all_relationships])
        
        # If DataFrame is empty, return empty result
        if df.empty:
            return pd.DataFrame(columns=[
                'year', 'total_volume', 'eu_volume', 'eu_ratio',
                'num_farmers', 'num_middlemen', 'num_exporters',
                'avg_middlemen_per_farmer', 'avg_exporters_per_middleman'
            ])
        
        # Group by year and calculate metrics
        evolution_data = df.groupby('year').agg({
            'amount_kg': 'sum',
            'farmer_id': 'nunique',
            'middleman_id': 'nunique',
            'exporter_id': 'nunique'
        }).reset_index()
        
        # Calculate EU volumes
        eu_volumes = df[df['sold_to_eu'] == True].groupby('year')['amount_kg'].sum().reset_index()
        eu_volumes.columns = ['year', 'eu_volume']
        
        # Merge EU volumes
        evolution_data = evolution_data.merge(eu_volumes, on='year', how='left')
        evolution_data['eu_volume'] = evolution_data['eu_volume'].fillna(0)
        
        # Rename columns
        evolution_data.rename(columns={
            'amount_kg': 'total_volume',
            'farmer_id': 'num_farmers',
            'middleman_id': 'num_middlemen',
            'exporter_id': 'num_exporters'
        }, inplace=True)
        
        # Calculate relationship averages
        farmer_relationships = df.groupby(['year', 'farmer_id'])['middleman_id'].nunique().reset_index()
        middleman_relationships = df.groupby(['year', 'middleman_id'])['exporter_id'].nunique().reset_index()
        
        avg_middlemen = farmer_relationships.groupby('year')['middleman_id'].mean().reset_index()
        avg_exporters = middleman_relationships.groupby('year')['exporter_id'].mean().reset_index()
        
        evolution_data = evolution_data.merge(
            avg_middlemen.rename(columns={'middleman_id': 'avg_middlemen_per_farmer'}),
            on='year'
        )
        evolution_data = evolution_data.merge(
            avg_exporters.rename(columns={'exporter_id': 'avg_exporters_per_middleman'}),
            on='year'
        )
        
        # Calculate EU ratio
        evolution_data['eu_ratio'] = evolution_data['eu_volume'] / evolution_data['total_volume']
        
        return evolution_data

    def analyze_geographic_distribution(
        self,
        year: int,
        country_id: str
    ) -> pd.DataFrame:
        """Analyze trading patterns by geography."""
        relationships = self.trading_registry.get_by_year_and_country(year, country_id)
        geographies = self.geography_registry.get_by_country(country_id)
        
        # Create geography lookup
        geo_lookup = {g.id: g for g in geographies}
        
        # Convert relationships to DataFrame
        df = pd.DataFrame([r.to_dict() for r in relationships])
        
        # Add geography information - Fix the geography_id extraction
        df['geography_id'] = df['farmer_id'].apply(lambda x: '_'.join(x.split('_')[:-1]))
        
        # Ensure geography_id exists in lookup before mapping
        valid_geos = df['geography_id'].isin(geo_lookup.keys())
        if not valid_geos.all():
            invalid_geos = df[~valid_geos]['geography_id'].unique()
            print(f"Warning: Found invalid geography IDs: {invalid_geos}")
            df = df[valid_geos]
        
        df['geography_name'] = df['geography_id'].map(lambda x: geo_lookup[x].name)
        df['producing_area'] = df['geography_id'].map(lambda x: geo_lookup[x].producing_area_name)
        
        # Group by geography
        geo_stats = df.groupby(['geography_id', 'geography_name', 'producing_area']).agg({
            'amount_kg': 'sum',
            'farmer_id': 'nunique',
            'middleman_id': 'nunique',
            'exporter_id': 'nunique',
            'sold_to_eu': ['sum', 'mean']
        }).round(2)
        
        geo_stats.columns = [
            'total_volume',
            'num_farmers',
            'num_middlemen',
            'num_exporters',
            'eu_volume',
            'eu_ratio'
        ]
        
        return geo_stats.reset_index()

    def generate_network_metrics(
        self,
        year: int,
        country_id: str
    ) -> Dict:
        """Calculate network metrics for the trading relationships."""
        relationships = self.trading_registry.get_by_year_and_country(year, country_id)
        df = pd.DataFrame([r.to_dict() for r in relationships])
        
        metrics = {
            'farmer_concentration': self._calculate_concentration(
                df.groupby('farmer_id')['amount_kg'].sum()
            ),
            'middleman_concentration': self._calculate_concentration(
                df.groupby('middleman_id')['amount_kg'].sum()
            ),
            'exporter_concentration': self._calculate_concentration(
                df.groupby('exporter_id')['amount_kg'].sum()
            ),
            'relationship_density': len(df) / (
                df['farmer_id'].nunique() * 
                df['middleman_id'].nunique() * 
                df['exporter_id'].nunique()
            ),
            'avg_chain_length': self._calculate_avg_chain_length(df)
        }
        
        return metrics

    def plot_trade_evolution(
        self,
        country_id: str,
        output_dir: Optional[Path] = None
    ) -> None:
        """Generate plots showing the evolution of trading patterns."""
        evolution_data = self.analyze_trade_evolution(country_id)
        
        # Create figure with subplots
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        
        # Volume trends
        ax1 = axes[0, 0]
        evolution_data.plot(
            x='year',
            y=['total_volume', 'eu_volume'],
            ax=ax1,
            title='Trading Volume Evolution'
        )
        ax1.set_ylabel('Volume (kg)')
        
        # Actor counts
        ax2 = axes[0, 1]
        evolution_data.plot(
            x='year',
            y=['num_farmers', 'num_middlemen', 'num_exporters'],
            ax=ax2,
            title='Supply Chain Actors'
        )
        ax2.set_ylabel('Count')
        
        # Average relationships
        ax3 = axes[1, 0]
        evolution_data.plot(
            x='year',
            y=['avg_middlemen_per_farmer', 'avg_exporters_per_middleman'],
            ax=ax3,
            title='Relationship Patterns'
        )
        ax3.set_ylabel('Average Count')
        
        # Average volumes
        ax4 = axes[1, 1]
        evolution_data.plot(
            x='year',
            y='avg_volume_per_farmer',
            ax=ax4,
            title='Average Volume per Farmer'
        )
        ax4.set_ylabel('Volume (kg)')
        
        plt.tight_layout()
        
        if output_dir:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            plt.savefig(output_dir / f'trade_evolution_{country_id}_{timestamp}.png')
        else:
            plt.show()

    def plot_geographic_distribution(
        self,
        year: int,
        country_id: str,
        output_dir: Optional[Path] = None
    ) -> None:
        """Generate plots showing geographic distribution of trade."""
        geo_stats = self.analyze_geographic_distribution(year, country_id)
        
        # Create figure with subplots
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        
        # Volume by geography
        ax1 = axes[0, 0]
        sns.barplot(
            data=geo_stats,
            x='geography_name',
            y='total_volume',
            ax=ax1
        )
        ax1.set_xticklabels(ax1.get_xticklabels(), rotation=45, ha='right')
        ax1.set_title('Trading Volume by Geography')
        
        # EU ratio by geography
        ax2 = axes[0, 1]
        sns.barplot(
            data=geo_stats,
            x='geography_name',
            y='eu_ratio',
            ax=ax2
        )
        ax2.set_xticklabels(ax2.get_xticklabels(), rotation=45, ha='right')
        ax2.set_title('EU Export Ratio by Geography')
        
        # Actor counts by geography
        ax3 = axes[1, 0]
        geo_stats_melted = pd.melt(
            geo_stats,
            id_vars=['geography_name'],
            value_vars=['num_farmers', 'num_middlemen', 'num_exporters'],
            var_name='actor_type',
            value_name='count'
        )
        sns.barplot(
            data=geo_stats_melted,
            x='geography_name',
            y='count',
            hue='actor_type',
            ax=ax3
        )
        ax3.set_xticklabels(ax3.get_xticklabels(), rotation=45, ha='right')
        ax3.set_title('Supply Chain Actors by Geography')
        
        # Volume by producing area
        ax4 = axes[1, 1]
        sns.boxplot(
            data=geo_stats,
            x='producing_area',
            y='total_volume',
            ax=ax4
        )
        ax4.set_xticklabels(ax4.get_xticklabels(), rotation=45, ha='right')
        ax4.set_title('Volume Distribution by Producing Area')
        
        plt.tight_layout()
        
        if output_dir:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            plt.savefig(output_dir / f'geographic_distribution_{country_id}_{year}_{timestamp}.png')
        else:
            plt.show()

    def _calculate_concentration(self, series: pd.Series) -> float:
        """Calculate Herfindahl-Hirschman Index (HHI) for concentration."""
        total = series.sum()
        if total == 0:
            return 0
        shares = series / total
        return (shares ** 2).sum()

    def _calculate_avg_chain_length(self, df: pd.DataFrame) -> float:
        """Calculate average number of links in supply chains."""
        chain_lengths = df.groupby('farmer_id').apply(
            lambda x: x.groupby('middleman_id')['exporter_id'].nunique().mean()
        )
        return chain_lengths.mean()

    def export_analytics(
        self,
        year: int,
        country_id: str,
        output_dir: Path
    ) -> None:
        """Export all analytics to files."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Export summary statistics
        summary = self.generate_year_summary(year, country_id)
        pd.DataFrame([summary['summary']]).to_csv(
            output_dir / f'summary_{country_id}_{year}_{timestamp}.csv',
            index=False
        )
        
        # Export geographic analysis
        geo_stats = self.analyze_geographic_distribution(year, country_id)
        geo_stats.to_csv(
            output_dir / f'geographic_analysis_{country_id}_{year}_{timestamp}.csv',
            index=False
        )
        
        # Export network metrics
        network_metrics = self.generate_network_metrics(year, country_id)
        pd.DataFrame([network_metrics]).to_csv(
            output_dir / f'network_metrics_{country_id}_{year}_{timestamp}.csv',
            index=False
        )
        
        # Generate and save plots
        self.plot_trade_evolution(country_id, output_dir)
        self.plot_geographic_distribution(year, country_id, output_dir)