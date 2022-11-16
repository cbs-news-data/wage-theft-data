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
