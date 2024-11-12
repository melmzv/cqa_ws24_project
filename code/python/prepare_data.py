# --- Header -------------------------------------------------------------------
# Prepare the pulled data for further analysis as per the requirements by Leuz et al.
#
# (C) Melisa Mazaeva -  See LICENSE file for details
# ------------------------------------------------------------------------------

import pandas as pd
import pickle
from utils import read_config, setup_logging

log = setup_logging()
def main():
    log.info("Preparing data for analysis ...")
    cfg = read_config('config/prepare_data_cfg.yaml')

    # Load the pulled data
    wrds_data = pd.read_csv(cfg['worldscope_sample_save_path'])
    initial_obs_count_pulled = len(wrds_data)
    initial_firm_count_pulled = len(wrds_data['item6105'].unique())
    log.info(f"Initial number of observations after pulling data: {initial_obs_count_pulled}")
    log.info(f"Initial number of firms after pulling data: {initial_firm_count_pulled}")

    # Check for duplicate firm-year observations
    dup_obs = wrds_data[wrds_data.duplicated(subset=['item6105', 'year_'], keep=False)]
    if not dup_obs.empty:
        log.warning(f"Found {dup_obs.shape[0]} duplicate firm-year observations. Removing duplicates.")
        wrds_data = wrds_data.drop_duplicates(subset=['item6105', 'year_'], keep='first')

    # Filter countries with at least 300 firm-year observations for key accounting variables as in paper
    filtered_countries_data, eliminated_countries = filter_countries(wrds_data)

    # Print eliminated countries
    if eliminated_countries:
        log.info(f"Countries eliminated after filtration: {', '.join(eliminated_countries)}")
    else:
        log.info("No countries were eliminated after filtration.")

    # Filter firms with at least three consecutive years of income statement and balance sheet information as in paper
    initial_firm_count = len(filtered_countries_data['item6105'].unique())
    initial_obs_count = len(filtered_countries_data)
    
    filtered_firms_data = filter_firms(filtered_countries_data)

    final_firm_count = len(filtered_firms_data['item6105'].unique())
    final_obs_count = len(filtered_firms_data)

    firms_dropped = initial_firm_count - final_firm_count
    obs_dropped = initial_obs_count - final_obs_count

    log.info(f"Firms dropped after filtration: {firms_dropped}")
    log.info(f"Firm-year observations dropped after filtration: {obs_dropped}")

    log.info(f"Number of observations after preparation: {final_obs_count}")
    log.info(f"Number of firms after preparation: {final_firm_count}")

    # Save the filtered dataset
    filtered_firms_data.to_csv(cfg['prepared_data_save_path'], index=False)

    log.info("Preparing data for analysis ... Done!")

    # Generate summary table for firm-year observations per country
    summary_table = filtered_firms_data.groupby('item6026').size().reset_index(name='# Firm-years')
    summary_table.columns = ['Country', '# Firm-years']
    
    # Compute mean, median, min, and max
    summary_stats = summary_table['# Firm-years'].describe()[['mean', '50%', 'min', 'max']]
    summary_stats.index = ['Mean', 'Median', 'Min', 'Max']
    summary_stats = summary_stats.reset_index()
    summary_stats.columns = ['Country', '# Firm-years']
    
    # Combine the summary table and stats
    table_1 = pd.concat([summary_table, summary_stats], ignore_index=True)

    # Remove decimal places
    table_1['# Firm-years'] = table_1['# Firm-years'].apply(lambda x: int(round(x)))
    
    # Insert an empty row after index 30
    empty_row = pd.DataFrame([['', '']], columns=table_1.columns)
    table_1_with_blank = pd.concat([table_1.iloc[:31], empty_row, table_1.iloc[31:]], ignore_index=True)
    
    # Save Table 1 to a pickle file
    results = {
        "table_1": table_1_with_blank
    }
    with open(cfg['table_1_save_path'], 'wb') as f:
        pickle.dump(results, f)
    
    log.info(f"Table 1 saved to {cfg['table_1_save_path']}")

def filter_countries(df):
    key_vars = ['item2999', 'item1001', 'item1250', 'item1651']  # Total Assets, Net Sales, Operating Income, Net Income
    grouped = df.groupby('item6026')
    country_filter = grouped.filter(lambda x: all(x[key_vars].count() >= 300))
    eliminated_countries = set(df['item6026'].unique()) - set(country_filter['item6026'].unique())
    return country_filter, list(eliminated_countries)

def filter_firms(df):
    key_vars = ['item2999', 'item1001', 'item1250', 'item1651']
    df = df.dropna(subset=key_vars)
    
    def has_three_consecutive_years(group):
        group = group.sort_values('year_')
        consecutive_years = (group['year_'].diff() == 1).astype(int).rolling(window=3).sum()
        return (consecutive_years >= 2).any()

    firm_filter = df.groupby('item6105').filter(has_three_consecutive_years)
    return firm_filter

if __name__ == "__main__":
    main()