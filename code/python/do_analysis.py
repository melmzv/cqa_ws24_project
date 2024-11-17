# We start by loading the libraries that we will use in this analysis.
import os
import pandas as pd
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
    cr4_shares = calculate_cr4_market_share(prepared_data)
    eu_shares = calculate_eu_level_market_shares(prepared_data)
    
    # Combine results into one DataFrame
    market_shares = combine_market_shares(big4_shares, kap10_shares, cr4_shares, eu_shares)
    
    # Save the aggregated market shares to a CSV file
    save_market_shares(market_shares, cfg['aggregated_data_save_path'])
    
    log.info("Performing main analysis...Done!")

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
    country_totals = df.groupby('trans_report_auditor_state')['number_of_disclosed_pies'].sum().reset_index()
    country_totals.columns = ['trans_report_auditor_state', 'total_pie_audits']
    
    big4_df = df[df['network_group'] == 'Big 4']
    big4_totals = big4_df.groupby('trans_report_auditor_state')['number_of_disclosed_pies'].sum().reset_index()
    big4_totals.columns = ['trans_report_auditor_state', 'big4_pie_audits']
    
    big4_shares = big4_totals.merge(country_totals, on='trans_report_auditor_state')
    big4_shares['big4_market_share'] = (big4_shares['big4_pie_audits'] / big4_shares['total_pie_audits']) * 100
    
    return big4_shares[['trans_report_auditor_state', 'big4_market_share']]

def calculate_kap10_market_share(df):
    """
    Calculate market share for 10KAP by country.
    """
    country_totals = df.groupby('trans_report_auditor_state')['number_of_disclosed_pies'].sum().reset_index()
    country_totals.columns = ['trans_report_auditor_state', 'total_pie_audits']
    
    kap10_df = df[df['network_group'] == '10KAP']
    kap10_totals = kap10_df.groupby('trans_report_auditor_state')['number_of_disclosed_pies'].sum().reset_index()
    kap10_totals.columns = ['trans_report_auditor_state', 'kap10_pie_audits']
    
    kap10_shares = kap10_totals.merge(country_totals, on='trans_report_auditor_state')
    kap10_shares['kap10_market_share'] = (kap10_shares['kap10_pie_audits'] / kap10_shares['total_pie_audits']) * 100
    
    return kap10_shares[['trans_report_auditor_state', 'kap10_market_share']]

def calculate_cr4_market_share(df):
    """
    Calculate the CR4 market share for the four largest audit firms in each country.
    """
    country_totals = df.groupby('trans_report_auditor_state')['number_of_disclosed_pies'].sum().reset_index()
    country_totals.columns = ['trans_report_auditor_state', 'total_pie_audits']
    
    firm_totals = (
        df.groupby(['trans_report_auditor_state', 'auditor_fkey'])['number_of_disclosed_pies']
        .sum()
        .reset_index()
    )
    
    top_4_audits = firm_totals.groupby('trans_report_auditor_state').apply(
        lambda x: x.nlargest(4, 'number_of_disclosed_pies')['number_of_disclosed_pies'].sum()
    ).reset_index(name='cr4_pie_audits')
    
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
    
    firm_totals = df.groupby('auditor_fkey')['number_of_disclosed_pies'].sum().nlargest(4).sum()
    
    eu_market_shares = pd.DataFrame({
        'trans_report_auditor_state': ['EU'],
        'big4_market_share': [big4_total / eu_totals * 100],
        'kap10_market_share': [kap10_total / eu_totals * 100],
        'cr4_market_share': [firm_totals / eu_totals * 100]
    })
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

if __name__ == "__main__":
    main()