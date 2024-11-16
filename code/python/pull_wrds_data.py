# --- Header -------------------------------------------------------------------
# See LICENSE file for details
#
# This code pulls data from WRDS Audit Analytics Database 
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
    wrds_data.to_csv(cfg['audit_analytics_save_path'], index=False)


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
        wrds_username = input('Please provide a WRDS username: ')
        wrds_password = getpass(
            'Please provide a WRDS password (it will not show as you type): ')
        return {'wrds_username': wrds_username, 'wrds_password': wrds_password}

def pull_wrds_data(cfg, wrds_authentication):
    '''
    Pulls WRDS Audit Europe Transparency Report data.
    '''
    db = wrds.Connection(
        wrds_username=wrds_authentication['wrds_username'], wrds_password=wrds_authentication['wrds_password']
    )

    log.info('Logged on to WRDS ...')

    # Select only the required variables from Transparency Reports
    selected_vars_str = ', '.join(cfg['selected_vars'])
    # Apply filter for 2021 and specific countries
    country_filter = ', '.join([f"'{country}'" for country in cfg['included_countries']])
    query = f"""
        SELECT {selected_vars_str}
        FROM audit_europe.feed76_transparency_reports
        WHERE REPORT_YEAR = 2021
        AND TRANS_REPORT_AUDITOR_STATE IN ({country_filter})
    """

    log.info("Pulling Transparency Report data ... ")
    wrds_data = db.raw_sql(query)
    log.info("Pulling Transparency Report data ... Done!")

    db.close()
    log.info("Disconnected from WRDS")

    return wrds_data

if __name__ == '__main__':
    main()