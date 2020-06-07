import requests
import typing
import pandas as pd
import argparse
from pathlib import Path
import logging
from . api_key import API_KEY
from . common import flip_and_set_time, concat_and_cleanup
import os
import time



root = logging.getLogger()
root.setLevel(logging.INFO)

BASE_URL = f'https://api.opendota.com/api/leagues?api_key={API_KEY}'

def get_league_data( max_retry: int = 10) -> pd.DataFrame:
    req_url = BASE_URL
    counter = 0
    while counter < max_retry:
        req = requests.get(url=req_url)
        if req.status_code == requests.codes.ok:
            return pd.read_json(req.text, orient='columns')
        counter += 1
        time.sleep(1)
    error_string = f"Max retries reached"
    logging.info(error_string)
    raise requests.exceptions.ConnectionError(error_string)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="script to collect pro match data")
    parser.add_argument('file_name', type=str)

    args = parser.parse_args()

    p = Path(os.getcwd() + f'/data/{args.file_name}')

    df = get_league_data()
    df.to_csv(p)

    p = Path(os.getcwd() + f'/data/premium_{args.file_name}')
    df = df[df['tier'] == 'premium']
    df.to_csv(p)
