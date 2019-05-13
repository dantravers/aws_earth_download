import configparser
import pandas as pd
import datetime
import boto3
import os

from aws_earth_fns import fetch_messages


def main():
    # read config
    my_config = configparser.ConfigParser()
    my_config.read('C:/Users/Dan Travers/Documents/GitHub/aws_earth_download/src/aws_config.ini')
    # set values from config
    aws_access_key_id = my_config['aws']['aws_access_key_id']
    aws_secret_access_key = my_config['aws']['aws_secret_access_key']
    mogreps_url = my_config['aws']['mogreps_url']
    ukv_url = my_config['aws']['ukv_url']
    region_name = my_config['aws']['region_name']
    variables = [x.strip() for x in my_config['aws']['variables'].split(',')]
    ukv_store_path = my_config['local_config']['ukv_store_path']
    mogreps_uk_store_path = my_config['local_config']['mogreps_uk_store_path']
    
    client = boto3.client(
        'sqs',
        aws_access_key_id = aws_access_key_id,
        aws_secret_access_key = aws_secret_access_key,
        region_name = region_name)
    
    # download MOGREPS messages
    mdf = pd.DataFrame([])
    mdf = mdf.append(fetch_messages(mogreps_url, client, variables))
    save_location = os.path.join(mogreps_uk_store_path, \
        'mogreps_sqs_messages_{}.csv'.format(datetime.datetime.now().strftime("%Y-%m-%dT%H-%M")))
    mdf.to_csv(save_location)

    # download UK-V messages
    mdf = pd.DataFrame([])
    mdf = mdf.append(fetch_messages(ukv_url, client, variables))
    save_location = os.path.join(ukv_store_path, \
        'ukv_sqs_messages_{}.csv'.format(datetime.datetime.now().strftime("%Y-%m-%dT%H-%M")))
    mdf.to_csv(save_location)

if __name__ == "__main__":
    main()