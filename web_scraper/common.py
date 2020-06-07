import typing
import pandas as pd
import logging

root = logging.getLogger()
root.setLevel(logging.INFO)


def flip_and_set_time(df: pd.DataFrame, time_col: str = 'start_time') -> pd.DataFrame:
    df = df.iloc[::-1]
    return df.set_index(time_col).sort_index()

def concat_and_cleanup(dfs: typing.List[pd.DataFrame]) -> pd.DataFrame:
    joined = pd.concat(dfs)
    if not joined.index.is_monotonic_increasing:
        logging.info("index not monotonic -- starting sort")
        joined = joined.sort_index().drop_duplicates()

    assert joined.index.is_monotonic_increasing, "Somehow index is not monotonically increasing"
    return joined