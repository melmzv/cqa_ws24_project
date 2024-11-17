# We start by loading the libraries that we will use in this analysis.
import pickle
import pandas as pd
from utils import read_config, setup_logging

# Set up logging
log = setup_logging()

def main():
    log.info("Performing main analysis...")
    
    # Load the configuration file
    cfg = read_config('config/do_analysis_cfg.yaml')
    
    # Load the prepared data using the path from the config file
    prepared_data = load_data(cfg['prepared_data_save_path'])
    
    # Calculate market shares for Big 4, 10KAP, and CR4
    market_shares = calculate_market_shares(prepared_data)
    
    # Save the aggregated market shares to a CSV file
    save_market_shares(market_shares, cfg['aggregated_data_save_path'])
    
    log.info("Performing main analysis...Done!")

def load_data(data_path):
    """
    Load the prepared transparency data from the specified path.
    """
    log.info(f"Loading prepared data from {data_path}...")
    df = pd.read_csv(data_path)
    return df

def calculate_market_shares(df):
    """
    Calculate market shares for Big 4, 10KAP, and CR4 for each country and for the EU as a whole.
    """
    # Group by country and calculate total PIE audits
    country_totals = (
        df.groupby('trans_report_auditor_state')['number_of_disclosed_pies']
        .sum()
        .reset_index(name='total_pie_audits')
    )
    
    # Big 4 Market Share
    big4_shares = calculate_group_market_share(df, 'Big 4', country_totals)
    
    # 10KAP Market Share
    kap10_shares = calculate_group_market_share(df, '10KAP', country_totals)
    
    # CR4 Market Share
    cr4_shares = calculate_cr4_market_share(df, country_totals)
    
    # Combine all market shares into one DataFrame
    market_shares = big4_shares.merge(kap10_shares, on='trans_report_auditor_state', how='outer')
    market_shares = market_shares.merge(cr4_shares, on='trans_report_auditor_state', how='outer')
    
    # Calculate EU-level market shares
    eu_market_shares = calculate_eu_level_market_shares(df)
    market_shares = pd.concat([market_shares, eu_market_shares], ignore_index=True)
    
    log.info("Market shares calculated successfully.")
    return market_shares

def calculate_group_market_share(df, group_name, country_totals):
    """
    Calculate market share for a specific network group (Big 4 or 10KAP).
    """
    group_df = df[df['network_group'] == group_name]
    group_totals = (
        group_df.groupby('trans_report_auditor_state')['number_of_disclosed_pies']
        .sum()
        .reset_index(name=f'{group_name.lower()}_pie_audits')
    )
    group_shares = group_totals.merge(country_totals, on='trans_report_auditor_state')
    group_shares[f'{group_name.lower()}_market_share'] = (
        group_shares[f'{group_name.lower()}_pie_audits'] / group_shares['total_pie_audits'] * 100
    )
    return group_shares[['trans_report_auditor_state', f'{group_name.lower()}_market_share']]

def calculate_cr4_market_share(df, country_totals):
    """
    Calculate the CR4 market share for the four largest audit firms in each country.
    """
    # Group by country and auditor to get PIE audits by firm
    firm_totals = (
        df.groupby(['trans_report_auditor_state', 'auditor_fkey'])['number_of_disclosed_pies']
        .sum()
        .reset_index()
    )
    
    # Identify the top 4 firms in each country
    top_4_audits = firm_totals.groupby('trans_report_auditor_state').apply(
        lambda x: x.nlargest(4, 'number_of_disclosed_pies')['number_of_disclosed_pies'].sum()
    ).reset_index(name='cr4_pie_audits')
    
    # Merge with country totals and calculate CR4 market share
    cr4_shares = top_4_audits.merge(country_totals, on='trans_report_auditor_state')
    cr4_shares['cr4_market_share'] = (cr4_shares['cr4_pie_audits'] / cr4_shares['total_pie_audits']) * 100
    return cr4_shares[['trans_report_auditor_state', 'cr4_market_share']]

def calculate_eu_level_market_shares(df):
    """
    Calculate EU-level market shares for Big 4, 10KAP, and CR4.
    """
    eu_totals = df['number_of_disclosed_pies'].sum()
    big4_total = df[df['network_group'] == 'Big 4']['number_of_disclosed_pies'].sum()
    kap10_total = df[df['network_group'] == '10KAP']['number_of_disclosed_pies'].sum()
    
    # Calculate CR4 at the EU level
    firm_totals = df.groupby('auditor_fkey')['number_of_disclosed_pies'].sum().nlargest(4).sum()
    
    # Assemble EU-level market shares
    eu_market_shares = pd.DataFrame({
        'trans_report_auditor_state': ['EU'],
        'big4_market_share': [big4_total / eu_totals * 100],
        'kap10_market_share': [kap10_total / eu_totals * 100],
        'cr4_market_share': [firm_totals / eu_totals * 100]
    })
    return eu_market_shares

def save_market_shares(df, save_path):
    """
    Save the aggregated market shares to a CSV file.
    """
    log.info(f"Saving aggregated market shares to {save_path}...")
    df.to_csv(save_path, index=False)
    log.info(f"Market shares saved to {save_path}.")

if __name__ == "__main__":
    main()