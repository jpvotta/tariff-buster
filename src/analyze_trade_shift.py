import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

print("Loading data from local Parquet file...")
df = pd.read_parquet('/Users/johnvotta/code/tariff-buster/src/tf_by_ht-2024_01-2025_08')

print(f"Data loaded: {df.shape[0]:,} rows")
print(f"\nFirst few rows:")
print(df.head())

def main():
    global df

    # Convert data types
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)
    df["import_value_monthly"] = df["import_value_monthly"].astype(int)

    # ===== Time Series Plot =====
    print("\n" + "="*80)
    print("Creating time series plot...")

    countries_of_interest = [
        'CHINA',
        'VIETNAM',
        'MALAYSIA',
        'THAILAND',
        'INDONESIA',
        'SINGAPORE',
        'PHILIPPINES',
        'TAIWAN'
    ]

    df_filtered = df[df['country_name'].isin(countries_of_interest)].copy()
    df_grouped = df_filtered.groupby(['year', 'month', 'country_name'])['import_value_monthly'].sum().reset_index()
    df_grouped['date'] = pd.to_datetime(df_grouped[['year', 'month']].assign(day=1))
    df_grouped = df_grouped.sort_values('date')

    fig, ax = plt.subplots(figsize=(16, 8))

    for country in countries_of_interest:
        country_data = df_grouped[df_grouped['country_name'] == country]
        if not country_data.empty:
            ax.plot(country_data['date'],
                    country_data['import_value_monthly'] / 1e9,
                    marker='o',
                    markersize=3,
                    linewidth=2,
                    label=country,
                    alpha=0.8)

    ax.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax.set_ylabel('Monthly Import Value (Billions USD)', fontsize=12, fontweight='bold')
    ax.set_title('US Monthly Imports from China and Neighboring Countries', fontsize=14, fontweight='bold', pad=20)
    ax.legend(loc='upper left', fontsize=10, framealpha=0.9)
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_minor_locator(mdates.MonthLocator((1, 4, 7, 10)))
    plt.xticks(rotation=45, ha='right')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:.1f}B'))
    plt.tight_layout()
    plt.savefig('/Users/johnvotta/code/tariff-buster/src/trade_timeseries.png', dpi=150, bbox_inches='tight')
    print("✓ Saved trade_timeseries.png")

    # ===== Analyze China vs Vietnam Shift =====
    print("\n" + "="*80)
    print("Analyzing China → Vietnam shift starting in 2025...")

    # Filter for China and Vietnam only
    df_cv = df[df['country_name'].isin(['CHINA', 'VIETNAM'])].copy()

    # Compare 2024 vs 2025 by HS code
    df_2024 = df_cv[df_cv['year'] == 2024].groupby(['hs_code', 'hs_description', 'country_name'])['import_value_monthly'].sum().reset_index()
    df_2025 = df_cv[df_cv['year'] == 2025].groupby(['hs_code', 'hs_description', 'country_name'])['import_value_monthly'].sum().reset_index()

    # Pivot to get China and Vietnam as columns
    df_2024_pivot = df_2024.pivot_table(
        index=['hs_code', 'hs_description'],
        columns='country_name',
        values='import_value_monthly',
        fill_value=0
    ).reset_index()

    df_2025_pivot = df_2025.pivot_table(
        index=['hs_code', 'hs_description'],
        columns='country_name',
        values='import_value_monthly',
        fill_value=0
    ).reset_index()

    # Merge 2024 and 2025 data
    df_comparison = df_2024_pivot.merge(
        df_2025_pivot,
        on=['hs_code', 'hs_description'],
        suffixes=('_2024', '_2025'),
        how='outer'
    ).fillna(0)

    # Calculate changes
    if 'CHINA_2024' in df_comparison.columns and 'CHINA_2025' in df_comparison.columns:
        df_comparison['china_change'] = df_comparison['CHINA_2025'] - df_comparison['CHINA_2024']
    else:
        df_comparison['china_change'] = 0

    if 'VIETNAM_2024' in df_comparison.columns and 'VIETNAM_2025' in df_comparison.columns:
        df_comparison['vietnam_change'] = df_comparison['VIETNAM_2025'] - df_comparison['VIETNAM_2024']
    else:
        df_comparison['vietnam_change'] = 0

    # Find products where China imports decreased AND Vietnam imports increased
    df_shift = df_comparison[
        (df_comparison['china_change'] < 0) &
        (df_comparison['vietnam_change'] > 0)
    ].copy()

    # Sort by magnitude of shift (sum of absolute changes)
    df_shift['shift_magnitude'] = abs(df_shift['china_change']) + df_shift['vietnam_change']
    df_shift = df_shift.sort_values('shift_magnitude', ascending=False)

    print(f"\n✓ Found {len(df_shift):,} HS codes where China imports decreased and Vietnam imports increased")
    print(f"\nTop 20 HS codes driving the China → Vietnam shift:")
    print("="*120)

    top_shifts = df_shift.head(20)
    for idx, row in top_shifts.iterrows():
        china_change_b = row['china_change'] / 1e9
        vietnam_change_b = row['vietnam_change'] / 1e9
        print(f"\nHS {row['hs_code']}: {row['hs_description'][:70]}")
        print(f"  China:   {china_change_b:+.2f}B USD")
        print(f"  Vietnam: {vietnam_change_b:+.2f}B USD")
        print(f"  Net shift magnitude: ${row['shift_magnitude']/1e9:.2f}B")

    # ===== Visualization of Top Shifts =====
    print("\n" + "="*80)
    print("Creating visualization of top product shifts...")

    top_10 = df_shift.head(10).copy()
    top_10['hs_label'] = top_10['hs_code'] + ': ' + top_10['hs_description'].str[:40]

    fig, ax = plt.subplots(figsize=(14, 8))

    x = range(len(top_10))
    width = 0.35

    china_bars = ax.barh([i - width/2 for i in x],
                          top_10['china_change'] / 1e9,
                          width,
                          label='China (decrease)',
                          color='#d62728',
                          alpha=0.8)

    vietnam_bars = ax.barh([i + width/2 for i in x],
                            top_10['vietnam_change'] / 1e9,
                            width,
                            label='Vietnam (increase)',
                            color='#2ca02c',
                            alpha=0.8)

    ax.set_yticks(x)
    ax.set_yticklabels(top_10['hs_label'], fontsize=9)
    ax.set_xlabel('Change in Import Value 2024→2025 (Billions USD)', fontsize=12, fontweight='bold')
    ax.set_title('Top 10 Products: China Decrease vs Vietnam Increase (2024→2025)',
                 fontsize=14, fontweight='bold', pad=20)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, linestyle='--', axis='x')
    ax.axvline(x=0, color='black', linewidth=0.8)

    plt.tight_layout()
    plt.savefig('/Users/johnvotta/code/tariff-buster/src/china_vietnam_shift.png', dpi=150, bbox_inches='tight')
    print("✓ Saved china_vietnam_shift.png")

    # ===== Summary Statistics =====
    print("\n" + "="*80)
    print("SUMMARY STATISTICS")
    print("="*80)

    total_china_decrease = df_shift['china_change'].sum() / 1e9
    total_vietnam_increase = df_shift['vietnam_change'].sum() / 1e9

    print(f"\nTotal China import decrease (products that shifted): ${abs(total_china_decrease):.2f}B")
    print(f"Total Vietnam import increase (products that shifted): ${total_vietnam_increase:.2f}B")
    print(f"Shift coverage: {(total_vietnam_increase / abs(total_china_decrease) * 100):.1f}% of China's decrease absorbed by Vietnam")

    print(f"\nNumber of distinct HS codes affected: {len(df_shift):,}")

    return df, df_shift

# Run the main function
df, df_shift = main()

print("\n" + "="*80)
print("✓ Analysis complete!")
print("="*80)
