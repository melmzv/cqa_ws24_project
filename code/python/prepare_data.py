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

    # Standardize Greece abbreviation
    transparency_data = standardize_greece_abbreviation(transparency_data)

    # Verify disclosed PIEs alignment with listed entities
    transparency_data = verify_disclosed_pies_alignment(transparency_data)

    # Check and handle blank values in vital fields
    vital_fields = ['transparency_report_fkey', 'entity_map_fkey', 'auditor_fkey', 'trans_report_auditor_state']
    transparency_data = check_and_handle_blanks(transparency_data, vital_fields)

    # Ensure rows with NUMBER_OF_DISCLOSED_PIES > 0
    transparency_data = filter_disclosed_pies(transparency_data)

    # Check for duplicates in entity_map_fkey
    check_for_joint_audits(transparency_data)

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

def standardize_greece_abbreviation(df):
    """
    Replace 'GR' with 'EL' for Greece in the trans_report_auditor_state column.
    """
    greece_count = df[df['trans_report_auditor_state'] == 'GR'].shape[0]
    if greece_count > 0:
        log.info(f"Found {greece_count} rows with 'GR' abbreviation for Greece. Replacing with 'EL'.")
        df['trans_report_auditor_state'] = df['trans_report_auditor_state'].replace('GR', 'EL')
    else:
        log.info("No 'GR' abbreviation found for Greece. No replacements made.")
    return df

def verify_disclosed_pies_alignment(df):
    """
    Verify that the number of unique entity_map_fkey matches number_of_disclosed_pies
    for each transparency_report_fkey.
    """
    # Group by transparency_report_fkey and count unique entity_map_fkey
    entity_counts = df.groupby('transparency_report_fkey')['entity_map_fkey'].nunique().reset_index()
    entity_counts.columns = ['transparency_report_fkey', 'entity_count']

    # Merge with original dataframe to compare counts
    merged_df = df.merge(entity_counts, on='transparency_report_fkey', how='left')

    # Check for mismatches
    overreported = merged_df[merged_df['entity_count'] > merged_df['number_of_disclosed_pies']]
    underreported = merged_df[merged_df['entity_count'] < merged_df['number_of_disclosed_pies']]

    if not overreported.empty:
        log.warning(f"{len(overreported)} transparency reports include more entities than disclosed PIEs.")
        log.warning("This indicates potential inclusion of non-PIE entities.")
        log.warning(overreported[['transparency_report_fkey', 'entity_count', 'number_of_disclosed_pies']].drop_duplicates())

    if not underreported.empty:
        log.warning(f"{len(underreported)} transparency reports include fewer entities than disclosed PIEs.")
        log.warning("This indicates potential missing PIE entities in the dataset.")
        log.warning(underreported[['transparency_report_fkey', 'entity_count', 'number_of_disclosed_pies']].drop_duplicates())

    if overreported.empty and underreported.empty:
        log.info("All transparency reports have consistent counts of disclosed PIEs and listed entities.")

    return df

def check_and_handle_blanks(df, fields):
    """
    Check for blank values in specified fields and remove rows with blanks.
    """
    initial_count = len(df)
    for field in fields:
        blank_count = df[field].isnull().sum()
        if blank_count > 0:
            log.warning(f"Found {blank_count} blank values in '{field}'. These rows will be dropped.")
            df = df[df[field].notnull()]
    final_count = len(df)
    log.info(f"Rows removed due to blank values: {initial_count - final_count}")
    return df


def filter_disclosed_pies(df):
    """
    Ensure that only rows with NUMBER_OF_DISCLOSED_PIES > 0 are included.
    """
    initial_count = len(df)
    df = df[df['number_of_disclosed_pies'] > 0]
    filtered_count = initial_count - len(df)
    if filtered_count > 0:
        log.info(f"{filtered_count} rows were filtered out because NUMBER_OF_DISCLOSED_PIES <= 0.")
    else:
        log.info("No rows were filtered out based on NUMBER_OF_DISCLOSED_PIES.")
    return df


def check_for_joint_audits(df):
    """
    Check for duplicates in entity_map_fkey to identify potential joint audits.
    """
    duplicate_count = df['entity_map_fkey'].duplicated().sum()
    if duplicate_count > 0:
        log.warning(f"Potential joint audits detected: {duplicate_count} entities appear in multiple transparency reports.")
    else:
        log.info("No potential joint audits detected. Each entity is unique in the dataset.")

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