# --- Header -------------------------------------------------------------------
# See LICENSE file for details
#
# This code pulls data from WRDS Worldscope Database
# ------------------------------------------------------------------------------
import os
from getpass import getpass
import dotenv

import pandas as pd
from utils import read_config, setup_logging
import wrds

log = setup_logging()

def main():
    '''
    Main function to pull data from WRDS.

    This function reads the configuration file, gets the WRDS login credentials, and pulls the data from WRDS.

    The data is then saved to a csv file.
    '''
    cfg = read_config('config/pull_data_cfg.yaml')
    wrds_login = get_wrds_login()
    wrds_data = pull_wrds_data(cfg, wrds_login)
    wrds_data.to_csv(cfg['worldscope_sample_save_path'], index=False)


def get_wrds_login():
    '''
    Gets the WRDS login credentials.
    '''
    if os.path.exists('secrets.env'):
        dotenv.load_dotenv('secrets.env')
        wrds_username = os.getenv('WRDS_USERNAME')
        wrds_password = os.getenv('WRDS_PASSWORD')
        return {'wrds_username': wrds_username, 'wrds_password': wrds_password}
    else:
        wrds_username = input('Please provide a wrds username: ')
        wrds_password = getpass(
            'Please provide a wrds password (it will not show as you type): ')
        return {'wrds_username': wrds_username, 'wrds_password': wrds_password}

def pull_wrds_data(cfg, wrds_authentication):
    '''
    Pulls WRDS access data.
    '''
    db = wrds.Connection(
        wrds_username=wrds_authentication['wrds_username'], wrds_password=wrds_authentication['wrds_password']
    )

    log.info('Logged on to WRDS ...')

    dyn_var_str = ', '.join(cfg['dyn_vars'])
    stat_var_str = ', '.join(cfg['stat_vars'])

    log.info("Pulling dynamic Worldscope data ... ")
    wrds_data_dynamic = db.raw_sql(
        f"SELECT {dyn_var_str} FROM tr_worldscope.wrds_ws_funda WHERE {cfg['cs_filter']}"
    )
    log.info("Pulling dynamic Worldscope data ... Done!")

    log.info("Pulling static Worldscope data ... ")
    wrds_data_static = db.raw_sql(f"SELECT {stat_var_str} FROM tr_worldscope.wrds_ws_company")
    log.info("Pulling static Worldscope data ... Done!")

    db.close()
    log.info("Disconnected from WRDS")

    # Display the number of rows with empty item6105 before filtering
    log.info(f"Rows with empty item6105 in dynamic data before filtering: {wrds_data_dynamic['item6105'].isna().sum()}")
    log.info(f"Rows with empty item6105 in static data before filtering: {wrds_data_static['item6105'].isna().sum()}")

    # Filter out rows with empty item6105 in both dynamic and static datasets, because this unique identifier is crucial 
    # for further data preparation step when we filter on firm/year observations
    wrds_data_dynamic = wrds_data_dynamic.dropna(subset=['item6105'])
    wrds_data_static = wrds_data_static.dropna(subset=['item6105'])

    # Merge the static and dynamic data to create one single data set by the unique item6105 (Worldscope Permanent ID)
    wrds_data = pd.merge(wrds_data_dynamic, wrds_data_static, on='item6105', how='inner')

    # Apply the filter for entity type to select only company rows in merged data based on Worldscope-specific Identifier advice
    # to retrieve only companies and drop average, exchange rate, ADR, security or a stock index.
    wrds_data = wrds_data[wrds_data['item6100'] == 'C']

    # Filter out financial institutions using SIC code as required by paper
    wrds_data = wrds_data.dropna(subset=['item7021'])
    wrds_data = wrds_data[(wrds_data['item7021'].astype(int) < 6000) | (wrds_data['item7021'].astype(int) > 6999)]

    # Apply the filter for specified countries given in the paper
    wrds_data = wrds_data[wrds_data['item6026'].isin(cfg['included_countries']) & ~wrds_data['item6026'].isin(cfg['excluded_countries'])]


    return wrds_data

if __name__ == '__main__':
    main()