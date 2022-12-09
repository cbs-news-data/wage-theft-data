"""calculates the case duration for each case in the dataframe"""

import sys
import pandas as pd


def get_end_date(df):
    """gets either the paid date or closed date for each case"""
    return df.assign(end_date=lambda d: d.date_closed.combine_first(d.date_paid))


def get_case_duration(df):
    """calculates the case duration for each case in the dataframe"""
    return df.assign(
        case_duration=lambda d: (get_end_date(d).end_date - d.date_opened).dt.days
    )


def main():
    """main function"""
    df = pd.read_csv(
        sys.stdin, dtype=str, parse_dates=["date_opened", "date_closed", "date_paid"]
    )
    df = get_case_duration(df)
    print(df.to_csv(index=False))


if __name__ == "__main__":
    main()
