import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import random
import seaborn as sns
from pathlib import Path


BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
SIMULATION_DATA_PATH = DATA_DIR / '_local/results/global_flows.parquet'
DATA = pd.read_parquet(SIMULATION_DATA_PATH)

CHART_COLOR = '#2E8B57'


def plot_kde(df, group_col, value_col, title):
    grouped = df.groupby(group_col)[value_col].sum()
    n = len(grouped)
    
    stats = {
        "Total Volume (bags)": grouped.sum() / 60,
        "Median Volume (kg)": grouped.median(),
        "Standard Deviation (kg)": grouped.std(),
        "Minimum Volume (kg)": grouped.min(),
        "Maximum Volume (kg)": grouped.max()
    }
    
    x_max = np.percentile(grouped, 99)
    
    sns.set_style("whitegrid")
    palette = sns.color_palette("coolwarm", as_cmap=True)
    fig, ax = plt.subplots(figsize=(10, 3))
    
    num_bins = 50 if len(grouped) < 100000 else 200
    counts, bins, _ = ax.hist(grouped, bins=num_bins, density=True, alpha=0.7, color=CHART_COLOR)
    density = counts * (bins[1] - bins[0]) * len(grouped)
    
    ax.set_title(f"{title} (n={n:,.0f})", fontsize=14, fontweight='bold', loc='left')
    ax.set_xlim(0, x_max)

    if x_max > 1e6:
        ax.set_xlabel("Total Weight (Millions kg)", fontsize=12)
        ax.xaxis.set_major_formatter(lambda x, p: f'{x/1e6:.1f}M')
    else:
        ax.set_xlabel("Total Weight (kg)", fontsize=12)
        ax.xaxis.set_major_formatter(lambda x, p: f'{x:,.0f}')
    
    ax.set_yticks([])
    ax.set_ylabel('')
    ax.grid(False)
    
    stats_text = "\n".join([f"{k}: {v:,.0f}" for k, v in stats.items()])
    ax.annotate(stats_text, xy=(0.95, 0.5), xycoords="axes fraction", ha="right", fontsize=10, 
                bbox=dict(boxstyle="round,pad=0.3", edgecolor='gray', facecolor='white', alpha=0.5))
    plt.tight_layout()
    return fig


def plot_farmers_per_exporter(farmers_per_exporter):
    farmer_counts = {exporter: len(farmers) for exporter, farmers in farmers_per_exporter.items()}
    fig, ax = plt.subplots(figsize=(10, 3))
    pd.Series(farmer_counts).sort_values(ascending=False).plot(kind="bar", width=.8, ax=ax, color=CHART_COLOR)
    ax.set_title("Number of Unique Farmers per Exporter")
    ax.set_xlabel("Exporter")
    
    total_exporters = len(farmers_per_exporter)
    xtick_interval = 10
    ax.set_xticks(range(0, total_exporters, xtick_interval))
    ax.set_xticklabels(range(0, total_exporters, xtick_interval), rotation=0)
    ax.set_ylabel("Unique Farmers")
    plt.tight_layout()
    return fig


def main():
    st.title("Supply Chain Simulator")
    
    countries = sorted(DATA["country"].unique())
    country = st.selectbox("Select a Country", countries)
    
    if country:
        country_df = DATA[DATA["country"] == country]
        
        # Calculate number of farmers early
        farmers_df = country_df[country_df["source"].str.startswith("F")]
        num_farmers = farmers_df['source'].nunique()
        
        # Traceability calculations
        middleman_to_farmers = (
            country_df[country_df["source"].str.startswith("F")]
            .groupby("target")["source"]
            .apply(set)
            .to_dict()
        )

        exporter_to_middlemen = (
            country_df[country_df["source"].str.startswith("M")]
            .groupby("target")["source"]
            .apply(set)
            .to_dict()
        )

        farmers_per_exporter = {
            exporter: set.union(
                *(middleman_to_farmers.get(middleman, set()) for middleman in middlemen)
            )
            for exporter, middlemen in exporter_to_middlemen.items()
        }
        
        # Random Sampling Analysis
        st.subheader("Random Sampling Analysis")
        total_exporters = len(farmers_per_exporter)
        N = total_exporters // 3
        random_exporters = random.sample(list(farmers_per_exporter.keys()), min(N, len(farmers_per_exporter)))
        random_farmers_union = set.union(*(farmers_per_exporter[exporter] for exporter in random_exporters))
        
        st.metric(
            label=f"Unique farmers across {N} random exporters",
            value=f"{len(random_farmers_union):,.0f}"
        )
        st.caption(f"out of {num_farmers:,.0f} total in country")
        
        st.subheader("Distribution Analysis")
        
        st.markdown("### Farmers")
        farmers_df = country_df[country_df["source"].str.startswith("F")]
        num_farmers = farmers_df['source'].nunique()
        fig_farmers = plot_kde(farmers_df, "source", "value", f"Farmers in {country}")
        st.pyplot(fig_farmers)

        st.markdown("### Middlemen")
        middlemen_df = country_df[country_df["source"].str.startswith("M")]
        fig_middlemen = plot_kde(middlemen_df, "source", "value", f"Middlemen in {country}")
        st.pyplot(fig_middlemen)

        st.markdown("### Exporters")
        exporters_df = country_df[country_df["target"].str.startswith("E")]
        fig_exporters = plot_kde(exporters_df, "target", "value", f"Exporters in {country}")
        st.pyplot(fig_exporters)

        st.subheader("Traceability Analysis")
        
        fig_farmers_per_exporter = plot_farmers_per_exporter(farmers_per_exporter)
        st.pyplot(fig_farmers_per_exporter)


if __name__ == "__main__":
    main()
