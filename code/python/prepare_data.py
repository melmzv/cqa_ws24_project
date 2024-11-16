# --- Header -------------------------------------------------------------------
# Prepare the pulled data for further analysis as per the requirements by EC Report (2024)
#
# (C) Melisa Mazaeva - See LICENSE file for details
# ------------------------------------------------------------------------------

import pandas as pd
from utils import read_config, setup_logging

log = setup_logging()

def main():
    log.info("Preparing data for analysis ...")
    cfg = read_config('config/prepare_data_cfg.yaml')

    # Load the pulled data
    transparency_data = pd.read_csv(cfg['audit_analytics_save_path'])
    initial_obs_count = len(transparency_data)
    log.info(f"Initial number of observations after pulling data: {initial_obs_count}")

    # Handle missing Auditor Network values
    transparency_data['auditor_network'] = transparency_data['auditor_network'].fillna('Other (Blank)')
    missing_networks = transparency_data['auditor_network'].value_counts().get('Other (Blank)', 0)
    log.info(f"Number of observations with missing Auditor Network: {missing_networks}")

    # Map duplicate auditor networks to standardized names
    transparency_data = standardize_auditor_network_names(transparency_data)

    # Map auditor networks into groups
    transparency_data = map_auditor_networks(transparency_data)

    # Save the prepared dataset
    transparency_data.to_csv(cfg['prepared_data_save_path'], index=False)
    log.info(f"Prepared data saved to {cfg['prepared_data_save_path']}")

    log.info("Preparing data for analysis ... Done!")

def standardize_auditor_network_names(df):
    """
    Standardize duplicate auditor network names to a single name.
    """
    duplicate_map = {
        '|Mazars Worldwide|Praxity Global Alliance|': '|Mazars Worldwide|',
        '|Praxity Global Alliance|TALENZ International|': '|Praxity Global Alliance|',
        '|Morison KSi|': '|Morison International|'
    }
    df['auditor_network'] = df['auditor_network'].replace(duplicate_map)
    log.info(f"Auditor Network names standardized: {duplicate_map}")
    return df

def map_auditor_networks(df):
    """
    Group auditor networks into Big Four, 10KAP (includes Big Four firms), Unaffiliated (not subset of BIG 4 and 10KAP), and Other (Blank).
    """
    # Define Big Four firms
    big4 = [
        '|Deloitte & Touche International|',
        '|Ernst & Young Global|',
        '|KPMG International|',
        '|PricewaterhouseCoopers International|'
    ]

    # Define 10KAP firms (in addition to Big Four)
    kap10 = big4 + [
        '|BDO International|',
        '|Grant Thornton International|',
        '|RSM Global (International)|',
        '|Mazars Worldwide|',
        '|Nexia International|',
        '|Baker Tilly International|',
    ]

    # Assign groups
    def map_group(name):
        if name in big4:
            return 'Big 4'
        elif name in kap10:
            return '10KAP'
        elif name == 'Other (Blank)':
            return 'Other (Blank)'
        else:
            return 'Unaffiliated'

    df['network_group'] = df['auditor_network'].apply(map_group)
    return df

if __name__ == "__main__":
    main()