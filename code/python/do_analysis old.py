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
    
    # Load the prepared financial data using the path from the config file
    financial_data = load_data(cfg['prepared_data_save_path'])

    # Calculate EM1
    country_em1, summary_stats_em1 = calculate_em1(financial_data)

    # Calculate EM2
    country_em2, summary_stats_em2 = calculate_em2(financial_data)

    # Calculate EM3
    country_em3, summary_stats_em3 = calculate_em3(financial_data)
    
    # Calculate EM4
    country_em4, summary_stats_em4 = calculate_em4(financial_data)

    # Calculate Aggregate Earnings Management Score and create final table
    final_table = calculate_aggregate(country_em1, country_em2, country_em3, country_em4)

    # Create the final combined table with both metrics and summary statistics
    final_combined_table = create_final_combined_table(final_table)

    # Save the final combined table using the path from the config file
    save_results(final_combined_table, cfg['results'])

    log.info("Performing main analysis...Done!")

def load_data(data_path):
    """
    Load the prepared financial data from the specified path.
    """
    df = pd.read_csv(data_path)
    return df

def calculate_em1(df):
    """
    Calculate EM1 for each firm and then take the country-level median.
    This function combines all necessary steps into one.
    """
    # Step 1: Calculate Accruals
    # Group by unique firm (item6105) and calculate diffs within each firm
    # If a firm does not report information on taxes payable or short-term debt,
    # then the change in both variables is assumed to be zero (per paper).
    df['delta_CA'] = df.groupby('item6105')['item2201'].diff()  # Change in total current assets
    df['delta_Cash'] = df.groupby('item6105')['item2003'].diff()  # Change in cash and cash equivalents
    df['delta_CL'] = df.groupby('item6105')['item3101'].diff()  # Change in total current liabilities
    df['delta_STD'] = df.groupby('item6105')['item3051'].diff().fillna(0)  # Apply fillna only for STD
    df['delta_TP'] = df.groupby('item6105')['item3063'].diff().fillna(0)  # Apply fillna only for TP
    df['Dep'] = df['item1151']  # Depreciation and amortization expense

    df['Accruals'] = (df['delta_CA'] - df['delta_Cash']) - (df['delta_CL'] - df['delta_STD'] - df['delta_TP']) - df['Dep']

    # Step 2: Calculate Operating Cash Flow (CFO) by subtracting accruals from operating income.
    df['CFO'] = df['item1250'] - df['Accruals']

    # Step 3: Calculate the standard deviations for Operating Income and CFO for each firm
    df['std_operating_income'] = df.groupby('item6105')['item1250'].transform('std')
    df['std_cfo'] = df.groupby('item6105')['CFO'].transform('std')

    # Step 4: Retrieve Lagged Total Assets
    df['lagged_total_assets'] = df.groupby('item6105')['item2999'].shift(1)

    # Step 5: Scale the standard deviations individually by lagged total assets
    df['scaled_std_operating_income'] = df['std_operating_income'] / df['lagged_total_assets']
    df['scaled_std_cfo'] = df['std_cfo'] / df['lagged_total_assets']

    # Step 6: Calculate EM1 as the ratio of the scaled standard deviations
    df['EM1'] = df['scaled_std_operating_income'] / df['scaled_std_cfo']

    # Step 7: Group by country (item6026) and calculate the median of EM1 for each country
    country_em1 = df.groupby('item6026')['EM1'].median().reset_index()
    # Round the EM1 results to three decimal places
    country_em1['EM1'] = country_em1['EM1'].round(3)

    # Step 8: Calculate summary statistics (mean, median, std, min, max) for EM1 across countries
    summary_stats_em1 = country_em1['EM1'].agg(['mean', 'median', 'std', 'min', 'max']).round(3)

    return country_em1, summary_stats_em1

def calculate_em2(df):
    """
    Calculate EM2, which is the Spearman correlation between the change in Accruals and CFO,
    both scaled by lagged total assets.
    """
    # Step 1: Calculate the change in Accruals and CFO
    df['delta_Accruals'] = df.groupby('item6105')['Accruals'].diff()
    df['delta_CFO'] = df.groupby('item6105')['CFO'].diff()

    # Step 2: Scale by lagged total assets
    df['scaled_delta_Accruals'] = df['delta_Accruals'] / df['lagged_total_assets']
    df['scaled_delta_CFO'] = df['delta_CFO'] / df['lagged_total_assets']

    # Step 3: Calculate EM2 as the Spearman correlation between scaled changes
    country_em2 = df.groupby('item6026').apply(
        lambda x: x[['scaled_delta_Accruals', 'scaled_delta_CFO']].corr(method='spearman').iloc[0, 1]
    ).reset_index()
    country_em2.columns = ['item6026', 'EM2']
    # Round the EM2 results to three decimal places
    country_em2['EM2'] = country_em2['EM2'].round(3)

    # Step 4: Calculate summary statistics (mean, median, std, min, max) for EM2 across countries
    summary_stats_em2 = country_em2['EM2'].agg(['mean', 'median', 'std', 'min', 'max']).round(3)

    return country_em2, summary_stats_em2

def calculate_em3(df):
    """
    Calculate EM3, which is the country’s median ratio of the absolute value of accruals
    and the absolute value of the cash flow from operations.
    """
    # Step 1: Calculate the absolute values of Accruals and CFO
    df['abs_Accruals'] = df['Accruals'].abs()
    df['abs_CFO'] = df['CFO'].abs()

    # Step 2: Calculate the ratio of abs(Accruals) to abs(CFO)
    df['EM3'] = df['abs_Accruals'] / df['abs_CFO']

    # Step 3: Group by country and calculate the median of EM3 for each country
    country_em3 = df.groupby('item6026')['EM3'].median().reset_index()

    # Round the EM3 results to three decimal places
    country_em3['EM3'] = country_em3['EM3'].round(3)

    # Step 4: Calculate summary statistics (mean, median, std, min, max) for EM3 across countries
    summary_stats_em3 = country_em3['EM3'].agg(['mean', 'median', 'std', 'min', 'max']).round(3)

    return country_em3, summary_stats_em3

def calculate_em4(df):
    """
    Calculate EM4, which is the ratio of the number of small profits to the number of small losses for each country.
    Small profits and losses are defined based on net earnings (item1651) scaled by lagged total assets.
    Only include countries with at least 5 small losses.
    """
    # Step 1: Calculate Net Earnings scaled by Lagged Total Assets
    df['scaled_net_earnings'] = df['item1651'] / df['lagged_total_assets']

    # Step 2: Identify Small Profits and Small Losses
    df['small_profits'] = ((df['scaled_net_earnings'] >= 0) & (df['scaled_net_earnings'] <= 0.01)).astype(int)
    df['small_losses'] = ((df['scaled_net_earnings'] >= -0.01) & (df['scaled_net_earnings'] < 0)).astype(int)

    # Step 3: Filter countries with at least 5 small losses
    country_counts = df.groupby('item6026')['small_losses'].sum()
    eligible_countries = country_counts[country_counts >= 5].index

    filtered_df = df[df['item6026'].isin(eligible_countries)]

    # Step 4: Calculate EM4 as the ratio of Small Profits to Small Losses for each eligible country
    country_em4 = filtered_df.groupby('item6026').apply(
        lambda x: x['small_profits'].sum() / max(1, x['small_losses'].sum())  # Avoid division by zero
    ).reset_index()
    country_em4.columns = ['item6026', 'EM4']
    # Round the EM4 results to three decimal places
    country_em4['EM4'] = country_em4['EM4'].round(3)

    # Step 5: Calculate summary statistics (mean, median, std, min, max) for EM4 across countries
    summary_stats_em4 = country_em4['EM4'].agg(['mean', 'median', 'std', 'min', 'max']).round(3)

    # Print how many countries were excluded
    excluded_countries = len(df['item6026'].unique()) - len(eligible_countries)
    print(f"{excluded_countries} countries were excluded due to having fewer than 5 small losses.")

    return country_em4, summary_stats_em4

def calculate_aggregate(df_em1, df_em2, df_em3, df_em4):
    """
    Calculate the aggregate earnings management score for each country.
    This function averages the ranks of EM1, EM2, EM3, and EM4.
    """
    # Step 1: Merge the EM1, EM2, EM3, and EM4 dataframes on the 'item6026' column (country identifier)
    combined_df = pd.merge(df_em1, df_em2, on='item6026')
    combined_df = pd.merge(combined_df, df_em3, on='item6026')
    combined_df = pd.merge(combined_df, df_em4, on='item6026')

    # Step 2: Rank each country by EM1, EM2, EM3, and EM4
    # Adjusting ranking based on whether higher scores imply more (+) or less (-) earnings management
    combined_df['Rank_EM1'] = combined_df['EM1'].rank(ascending=False)  # "-" indicates less EM with higher scores
    combined_df['Rank_EM2'] = combined_df['EM2'].rank(ascending=False)  # "-" indicates less EM with higher scores
    combined_df['Rank_EM3'] = combined_df['EM3'].rank(ascending=True)  # "+" indicates more EM with higher scores
    combined_df['Rank_EM4'] = combined_df['EM4'].rank(ascending=True)  # "+" indicates more EM with higher scores

    # Step 3: Calculate the aggregate score by averaging the ranks and round to one decimal place
    combined_df['Aggregate_EM_Score'] = combined_df[['Rank_EM1', 'Rank_EM2', 'Rank_EM3', 'Rank_EM4']].mean(axis=1).round(1)

    # Keep all relevant columns for the final table
    final_table = combined_df[['item6026', 'EM1', 'EM2', 'EM3', 'EM4', 'Aggregate_EM_Score']].sort_values(by='Aggregate_EM_Score', ascending=False).reset_index(drop=True)
    
    return final_table

def create_final_table_and_summary_stats(final_table):
    """
    Create the final table with EM1-EM4 and aggregate scores, and display the summary statistics separately.
    """
    # Calculate summary statistics for EM1-EM4 
    summary_stats = final_table[['EM1', 'EM2', 'EM3', 'EM4']].agg(['mean', 'median', 'std', 'min', 'max']).round(3)
    
    return final_table, summary_stats

def create_final_combined_table(final_table):
    """
    Create a final combined table with EM1-EM4, aggregate scores, and summary statistics.
    """
    # Calculate summary statistics for EM1-EM4
    summary_stats = final_table[['EM1', 'EM2', 'EM3', 'EM4']].agg(['mean', 'median', 'std', 'min', 'max']).round(3)
    
    # Rename the index of summary_stats to be more descriptive
    summary_stats.index = ['Mean', 'Median', 'Std', 'Min', 'Max']
    
    # Create a DataFrame for the summary stats with the same columns as final_table
    summary_stats_df = pd.DataFrame(
        summary_stats.values,
        columns=['EM1', 'EM2', 'EM3', 'EM4'],
        index=['Mean', 'Median', 'Std', 'Min', 'Max']
    ).reset_index()
    
    # Add the names in the 'item6026' column (which originally holds the country names)
    summary_stats_df['item6026'] = summary_stats_df['index']
    summary_stats_df['Aggregate_EM_Score'] = ''
    
    # Reorder columns to match the final_table structure and drop the 'index' column
    summary_stats_df = summary_stats_df[['item6026', 'EM1', 'EM2', 'EM3', 'EM4', 'Aggregate_EM_Score']]
    
    # Format the summary_stats_df to include trailing zeros for all numbers
    summary_stats_df[['EM1', 'EM2', 'EM3', 'EM4']] = summary_stats_df[['EM1', 'EM2', 'EM3', 'EM4']].applymap(lambda x: f"{x:.3f}")
    
    # Insert an empty row after index 30 like in paper
    empty_row = pd.DataFrame([['', '', '', '', '', '']], columns=final_table.columns)
    final_table_with_blank = pd.concat([final_table.iloc[:31], empty_row, final_table.iloc[31:]], ignore_index=True)
    
    # Format the final_table_with_blank to include trailing zeros for all numbers
    final_table_with_blank[['EM1', 'EM2', 'EM3', 'EM4']] = final_table_with_blank[['EM1', 'EM2', 'EM3', 'EM4']].applymap(lambda x: f"{float(x):.3f}" if x != '' else x)
    
    # Round to one decimal place like in paper
    final_table_with_blank['Aggregate_EM_Score'] = final_table_with_blank['Aggregate_EM_Score'].apply(lambda x: f"{float(x):.1f}" if x != '' else x)
    
    final_combined_table = pd.concat([final_table_with_blank, summary_stats_df], ignore_index=True)
    
    print("\nFinal Combined Table (with Summary Statistics):")
    print(final_combined_table)
    
    return final_combined_table

def save_results(final_combined_table, save_path):
    """
    Save the final combined table to a pickle file at the specified path.
    """
    with open(save_path, 'wb') as f:
        pickle.dump({
            'final_combined_table': final_combined_table
        }, f)

if __name__ == "__main__":
    main()