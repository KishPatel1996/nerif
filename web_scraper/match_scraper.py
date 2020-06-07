import requests
import typing
import pandas as pd
import time
import argparse
import os
from pathlib import Path
import logging
from . api_key import API_KEY
from . common import flip_and_set_time, concat_and_cleanup

root = logging.getLogger()
root.setLevel(logging.INFO)

BASE_URL = f'https://api.opendota.com/api/proMatches?api_key={API_KEY}'


def get_match_data(match_id: int = None, max_retry: int = 10) -> pd.DataFrame:
    req_url = BASE_URL
    if match_id:
        req_url = req_url + f"&less_than_match_id={match_id}"
    counter = 0
    while counter < max_retry:
        req = requests.get(url=req_url)
        if req.status_code == requests.codes.ok:
            return pd.read_json(req.text, orient='columns')
        counter += 1
        time.sleep(1)
    error_string = f"Max retries reached for match query with id {match_id}"
    logging.info(error_string)
    raise requests.exceptions.ConnectionError(error_string)


def drop_nan_and_convert_to_int(df: pd.DataFrame) -> pd.DataFrame:
    int_columns = ['radiant_team_id', 'dire_team_id']

    return df.dropna(subset=int_columns).astype({x: int for x in int_columns})


def initial_backup(match_data_str: str,
                   max_api_call: int,
                   start_from_prev_file: bool = False):

    os.makedirs(os.getcwd() + '/data', exist_ok=True)
    p = Path(os.getcwd() + f'/data/{match_data_str}')
    # if no match_id, starting from the most recent game
    match_id = None
    dfs = []
    if start_from_prev_file:
        logging.info(f"Retrieve saved data from {p}")

        # Assuming already sorted
        saved_df = pd.read_csv(p, index_col='start_time', parse_dates=True)
        match_id = saved_df['match_id'][0]
        dfs.append(saved_df)

    try:
        for _ in range(max_api_call):
            df = flip_and_set_time(get_match_data(match_id))
            match_id = df['match_id'][0]
            dfs.insert(0, df)
            logging.info("{} points added from {} to {}".format(
                df.shape[0], df.index[0], df.index[-1]))
    except requests.exceptions.ConnectionError:
        pass

    # Write to disk as a csv

    os.makedirs(os.getcwd() + '/data', exist_ok=True)

    joined = concat_and_cleanup(dfs)

    joined.to_csv(p)

    cleaned = drop_nan_and_convert_to_int(joined)
    cleaned_path = Path(os.getcwd() + f'/data/clean_{match_data_str}')
    cleaned.to_csv(cleaned_path)


def grab_latest_data(match_data_str: str, max_api_calls: int):
    p = Path(os.getcwd() + f'/data/{match_data_str}')

    logging.info(f"Retrieve saved data from {p}")

    # Assuming already sorted
    saved_df = pd.read_csv(p, index_col='start_time', parse_dates=True)

    saved_df_matches = saved_df['match_id'].values
    dfs = []
    match_id = None
    for _ in range(max_api_calls):
        df = flip_and_set_time(get_match_data(match_id))
        match_id = df['match_id'][0]
        dfs.insert(0, df)
        if match_id in saved_df_matches:
            logging.info(f"Duplicate match_id hit: {match_id}")
            break

    dfs.insert(0, saved_df)
    joined = concat_and_cleanup(dfs)
    joined.to_csv(p)

    cleaned = drop_nan_and_convert_to_int(joined)
    cleaned_path = Path(os.getcwd() + f'/data/clean_{match_data_str}')
    cleaned.to_csv(cleaned_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="script to collect pro match data")
    parser.add_argument('file_name', type=str)
    parser.add_argument('--update_data', action='store_true')
    parser.add_argument('--start_from_prev', action='store_true')
    parser.add_argument('--max_api_call', type=int, default=1000)

    args = parser.parse_args()
    print(args)
    if args.update_data:
        grab_latest_data(args.file_name, args.max_api_call)
    else:
        initial_backup(match_data_str=args.file_name,
                       start_from_prev_file=args.start_from_prev,
                       max_api_call=args.max_api_call)
