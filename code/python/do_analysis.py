# We start by loading the libraries that we will use in this analysis.
import os
import pickle
import pandas as pd
import matplotlib.pyplot as plt
from utils import read_config, setup_logging

# Set up logging
log = setup_logging()

def main():
    log.info("Performing main analysis...")
    
    # Load the configuration file
    cfg = read_config('config/do_analysis_cfg.yaml')
    
    # Load the prepared transparency data using the path from the config file
    prepared_data = load_data(cfg['prepared_data_save_path'])
    
    # Calculate market shares for each group
    big4_shares = calculate_big4_market_share(prepared_data)
    kap10_shares = calculate_kap10_market_share(prepared_data)
    cr4_shares = calculate_cr4_market_share(prepared_data, big4_shares)  # Pass big4_shares as an argument
    eu_shares = calculate_eu_level_market_shares(prepared_data)
    
    # Combine results into one DataFrame
    market_shares = combine_market_shares(big4_shares, kap10_shares, cr4_shares, eu_shares)
    
    # Save the aggregated market shares to a CSV file
    save_market_shares(market_shares, cfg['aggregated_data_save_path'])

    # Plot the results
    plot_market_shares(market_shares, cfg['figure_save_path'], cfg['pickle_save_path'])
    
    log.info("Performing main analysis and plotting...Done!")

def load_data(data_path):
    """
    Load the prepared transparency data from the specified path.
    """
    df = pd.read_csv(data_path)
    return df

def calculate_big4_market_share(df):
    """
    Calculate market share for Big 4 by country.
    """
    # Step 1: Calculate total PIE audits by counting rows per country
    country_totals = df.groupby('trans_report_auditor_state').size().reset_index(name='total_pie_audits')
    print("Country Totals (All Audits):")
    print(country_totals.head(9))
    
    # Step 2: Filter data for Big 4
    big4_df = df[df['network_group'] == 'Big 4']
    print("Filtered Big 4 Data:")
    print(big4_df.head())
    
    # Step 3: Calculate Big 4 PIE audits by counting rows per country
    big4_totals = big4_df.groupby('trans_report_auditor_state').size().reset_index(name='big4_pie_audits')
    print("Big 4 Totals (Audits by Country):")
    print(big4_totals.head(10))
    
    # Step 4: Merge with country totals and calculate market share
    big4_shares = big4_totals.merge(country_totals, on='trans_report_auditor_state')
    big4_shares['big4_market_share'] = (big4_shares['big4_pie_audits'] / big4_shares['total_pie_audits']) * 100
    print("Big 4 Market Shares:")
    print(big4_shares.head(10))
    
    # Step 5: Return final DataFrame
    return big4_shares[['trans_report_auditor_state', 'big4_market_share']]

def calculate_kap10_market_share(df):
    """
    Calculate market share for 10KAP by country, including Big 4 auditors.
    """
    # Step 1: Calculate total PIE audits by counting rows per country
    country_totals = df.groupby('trans_report_auditor_state').size().reset_index(name='total_pie_audits')
    print("Country Totals (All Audits):")
    print(country_totals.head())
    
    # Step 2: Filter data for 10KAP (including Big 4)
    kap10_df = df[df['network_group'].isin(['10KAP', 'Big 4'])]
    print("Filtered 10KAP Data (Including Big 4):")
    print(kap10_df.head())
    
    # Step 3: Calculate 10KAP PIE audits by counting rows per country
    kap10_totals = kap10_df.groupby('trans_report_auditor_state').size().reset_index(name='kap10_pie_audits')
    print("10KAP Totals (Audits by Country):")
    print(kap10_totals.head())
    
    # Step 4: Merge with country totals and calculate market share
    kap10_shares = kap10_totals.merge(country_totals, on='trans_report_auditor_state')
    kap10_shares['kap10_market_share'] = (kap10_shares['kap10_pie_audits'] / kap10_shares['total_pie_audits']) * 100
    print("10KAP Market Shares:")
    print(kap10_shares.head())
    
    # Step 5: Return final DataFrame
    return kap10_shares[['trans_report_auditor_state', 'kap10_market_share']]

def calculate_cr4_market_share(df, big4_shares):
    """
    Calculate the CR4 market share for the four largest audit firms in each country,
    and check for overlap with Big 4 market share.
    """
    # Step 1: Count statutory audits by firm within each country
    firm_totals = (
        df.groupby(['trans_report_auditor_state', 'auditor_fkey'])
        .size()
        .reset_index(name='audit_count')
    )
    print("Firm Totals (Audit Count by Firm and Country):")
    print(firm_totals.head())

    # Step 2: Identify the top 4 firms in each country and sum their audit counts
    top_4_audits = firm_totals.groupby('trans_report_auditor_state').apply(
        lambda x: x.nlargest(4, 'audit_count')['audit_count'].sum()
    ).reset_index(name='cr4_audit_count')
    print("Top 4 Audits (Summed for Each Country):")
    print(top_4_audits.head())

    # Step 3: Count total statutory audits per country
    country_totals = df.groupby('trans_report_auditor_state').size().reset_index(name='total_audit_count')
    print("Total Audits (By Country):")
    print(country_totals.head())

    # Step 4: Merge with total audits and calculate market share
    cr4_shares = top_4_audits.merge(country_totals, on='trans_report_auditor_state')
    cr4_shares['cr4_market_share'] = (cr4_shares['cr4_audit_count'] / cr4_shares['total_audit_count']) * 100
    print("CR4 Market Shares (By Country):")
    print(cr4_shares.head(9))

    # Step 5: Merge with Big 4 market shares to check overlap
    cr4_shares = cr4_shares.merge(big4_shares, on='trans_report_auditor_state', how='left')
    cr4_shares['overlap_with_big4'] = cr4_shares['cr4_market_share'] == cr4_shares['big4_market_share']

    print("CR4 Market Shares with Overlap Check (By Country):")
    print(cr4_shares[['trans_report_auditor_state', 'cr4_market_share', 'big4_market_share', 'overlap_with_big4']])

    # Step 6: Calculate the number of countries with overlap
    overlap_count = cr4_shares['overlap_with_big4'].sum()
    print(f"Number of countries with CR4 overlapping with Big 4: {overlap_count}")

    # Step 7: Return final DataFrame with overlap flag
    return cr4_shares[['trans_report_auditor_state', 'cr4_market_share', 'overlap_with_big4']]

def calculate_eu_level_market_shares(df):
    """
    Calculate EU-level market shares for Big 4, 10KAP, and CR4 using statutory audit counts.
    """
    # Step 1: Calculate total statutory audits in the EU by counting rows
    eu_totals = len(df)
    print(f"Total Statutory Audits in the EU: {eu_totals}")

    # Step 2: Filter data for Big 4 and calculate total audits
    big4_df = df[df['network_group'] == 'Big 4']
    big4_total = len(big4_df)
    print(f"Total Big 4 Audits in the EU: {big4_total}")

    # Step 3: Filter data for 10KAP (including Big 4) and calculate total audits
    kap10_df = df[df['network_group'].isin(['10KAP', 'Big 4'])]
    kap10_total = len(kap10_df)
    print(f"Total 10KAP Audits in the EU: {kap10_total}")

    # Step 4: Identify the top 4 firms in the EU (CR4) based on audit counts
    firm_totals = df.groupby('auditor_fkey').size().reset_index(name='audit_count')
    print("Total Audits by Firm in the EU:")
    print(firm_totals.head())

    top_4_audits = firm_totals.nlargest(4, 'audit_count')['audit_count'].sum()
    print(f"Total Audits by Top 4 Firms (CR4) in the EU: {top_4_audits}")

    # Step 5: Calculate market shares for Big 4, 10KAP, and CR4
    big4_market_share = (big4_total / eu_totals) * 100
    kap10_market_share = (kap10_total / eu_totals) * 100
    cr4_market_share = (top_4_audits / eu_totals) * 100
    print(f"Big 4 Market Share in the EU: {big4_market_share:.2f}%")
    print(f"10KAP Market Share in the EU: {kap10_market_share:.2f}%")
    print(f"CR4 Market Share in the EU: {cr4_market_share:.2f}%")

    # Step 6: Create a DataFrame with EU-level market shares
    eu_market_shares = pd.DataFrame({
        'trans_report_auditor_state': ['EU'],
        'big4_market_share': [big4_market_share],
        'kap10_market_share': [kap10_market_share],
        'cr4_market_share': [cr4_market_share]
    })

    # Step 7: Return the EU-level market shares DataFrame
    return eu_market_shares

def combine_market_shares(big4_shares, kap10_shares, cr4_shares, eu_shares):
    """
    Combine market shares for Big 4, 10KAP, CR4, and EU-level into one DataFrame.
    """
    market_shares = big4_shares.merge(kap10_shares, on='trans_report_auditor_state', how='outer')
    market_shares = market_shares.merge(cr4_shares, on='trans_report_auditor_state', how='outer')
    market_shares = pd.concat([market_shares, eu_shares], ignore_index=True)
    return market_shares

def save_market_shares(df, save_path):
    """
    Save the aggregated market shares to a CSV file.
    """
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    df.to_csv(save_path, index=False)
    log.info(f"Market shares saved to {save_path}.")

def plot_market_shares(market_shares, save_path, pickle_path):
    """
    Create a patterned bar chart for Big 4, CR4, and 10KAP market shares by country (including EU),
    and save the figure as both PNG and pickle formats.
    """
    # Sort the market shares to match the desired country order
    countries_order = ['DK', 'CY', 'FI', 'SE', 'LU', 'BE', 'EE', 'AT', 'NO', 'NL', 'IT',
                       'ES', 'IE', 'DE', 'MT', 'LT', 'HU', 'EU', 'LV', 'CZ', 'HR', 'SI',
                       'SK', 'FR', 'PL', 'PT', 'EL', 'RO', 'BG']
    market_shares['sort_order'] = market_shares['trans_report_auditor_state'].apply(lambda x: countries_order.index(x))
    market_shares = market_shares.sort_values(by='sort_order')
    
    # Extract data for plotting
    countries = market_shares['trans_report_auditor_state']
    big4 = market_shares['big4_market_share']
    cr4 = market_shares['cr4_market_share']
    kap10 = market_shares['kap10_market_share']
    
    # Define bar width and x-coordinates
    x = range(len(countries))
    bar_width = 0.25

    # Plot settings
    fig, ax = plt.subplots(figsize=(14, 7))

    # Add bars with appropriate colors
    for i, country in enumerate(countries):
        if country == 'EU':
            # Highlight EU with different red shades
            ax.bar(i, big4.iloc[i], width=bar_width, color='darkred', align='center', label='Big 4 (EU)' if i == 0 else "")
            ax.bar(i + bar_width, cr4.iloc[i], width=bar_width, color='red', align='center', label='CR4 (EU)' if i == 0 else "")
            ax.bar(i + 2 * bar_width, kap10.iloc[i], width=bar_width, color='lightcoral', align='center', label='10KAP (EU)' if i == 0 else "")
        else:
            # Default colors for other countries
            ax.bar(i, big4.iloc[i], width=bar_width, color='darkblue', align='center', label='Big 4' if i == 0 else "")
            ax.bar(i + bar_width, cr4.iloc[i], width=bar_width, color='lightblue', align='center', label='CR4' if i == 0 else "")
            ax.bar(i + 2 * bar_width, kap10.iloc[i], width=bar_width, color='gray', align='center', label='10KAP' if i == 0 else "")

    # Add labels, title, and legend
    ax.set_xlabel('Country', fontsize=12)
    ax.set_ylabel('Market Share (%)', fontsize=12)
    ax.set_xticks([i + bar_width for i in x])
    ax.set_xticklabels(countries, rotation=90)
    
    # Define legend below the graph
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15), fancybox=True, shadow=True, ncol=3)
    
    # Save the figure as a PNG
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    plt.tight_layout()
    plt.savefig(save_path)
    log.info(f"Figure saved to {save_path}.")
    
    # Save the figure as a pickle file
    os.makedirs(os.path.dirname(pickle_path), exist_ok=True)
    with open(pickle_path, 'wb') as f:
        pickle.dump(fig, f)
    log.info(f"Figure saved as pickle to {pickle_path}.")
    
    plt.show()
    
if __name__ == "__main__":
    main()