"""shared functions not specific to any one module that are used across multiple tasks"""

import pandas as pd


def get_coverage_df(df: pd.DataFrame, by_state=True) -> pd.DataFrame:
    """
    Gets a dataframe containing the percentage of rows containig non-null values
    """
    coverage_cols = [
        "case_status",
        "amount_claimed",
        "amount_assessed",
        "amount_paid",
        "date_opened",
        "date_closed",
    ]
    if by_state:
        coverage_df = df.groupby("state_name").size().to_frame("total_cases")
        for colname in coverage_cols:
            new_colname = f"cases_with_{colname}"
            coverage_df = (
                coverage_df.merge(
                    df.query(f"{colname}.notna()")
                    .groupby("state_name")[colname]
                    .count()
                    .to_frame(new_colname),
                    how="outer",
                    left_index=True,
                    right_index=True,
                )
                .assign(
                    **{
                        new_colname: lambda df: df[
                            new_colname  # pylint: disable=cell-var-from-loop
                        ]
                        / df["total_cases"]
                    }
                )
                .fillna(0)
            )
    else:
        coverage_df = df[coverage_cols].notna().mean()

    return coverage_df


def append_texas_amounts(df):
    """removes Texas from the dataframe and appends the separate amounts"""
    return pd.concat(
        [
            df.query('state_name != "Texas"'),
            (
                pd.read_excel(
                    "input/ORR_R005317-081222_from_CBS__C._Hacker__File_date___Amts.xlsx"
                )
                .iloc[:, 1:]
                .rename(
                    columns={
                        "CLAIMED": "amount_claimed",
                        "AWARD_AM": "amount_assessed",
                        "PAID": "amount_paid",
                    }
                )
                .assign(state_name="Texas")
                # only take cases with non-zero assessed amounts
                .query("amount_assessed > 0")
            ),
        ]
    )
