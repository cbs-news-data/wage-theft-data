"""
The data provided by the states is extremely messy and inconsistent. Some states provided
detailed data about each case: case statuses or dispositions, multiple dates, multiple
amount fields, etc. Others provided some of those data points, but not all,
while others provided very little.

This script will attempt to determine the status of a case using the available data
so that open, rejected/dismissed, withdrawn and other incomplete cases can be removed.
"""

import sys
from typing import Union
import numpy as np
import pandas as pd
from tqdm import tqdm
from shared_functions import get_coverage_df

tqdm.pandas()


def determine_case_completion(row: pd.Series, coverage_df: pd.DataFrame) -> bool:
    """determine whether the case is completed and was not denied/withdrawn/dismissed/etc.

    Args:
        row (pd.Series): row of dataframe to evaluate
        coverage_df (pd.DataFrame): dataframe of data for each state

    Returns:
        bool: True if case is completed, False otherwise
    """
    # if the case has any of the following statuses, it is not completed
    if row.case_status in [
        "open",
        "dismissed",
        "withdrawn",
        "pending appeal",
        "overturned",
    ]:
        return (False, "has open case status")

    # any of these are always considered completed, even if the record
    # is missing amounts
    if row.case_status in [
        "pending enforcement",
        "affirmed",
        "amount exceeds statutory limit",
    ]:
        return (True, "has final case status")

    # if the case is paid, it is always considered completed
    if pd.notna(row.amount_paid):
        return (True, "has amount paid")

    # if the case status is null OR "closed", use the amount fields to determine
    # whether it is closed or not
    if pd.isna(row.case_status) or row.case_status == "closed":
        return infer_case_status_from_amount(row, coverage_df)

    # otherwise fail, this should never be possible
    raise ValueError(
        f"Case completion could not be determined for the following row: {row}"
    )


def infer_case_status_from_amount(
    row: pd.Series, coverage_df: pd.DataFrame
) -> Union[tuple[bool, str], float]:
    """infer case status from amount fields

    Args:
        row (pd.Series): row of dataframe to evaluate
        coverage_df (pd.DataFrame): dataframe of data for each state

    Returns:
        Union[tuple[bool, str], float]: tuple of (True/False, reason for True/False) or NaN
    """
    # if assessed amount is available for this case, use that
    if pd.notna(row.amount_assessed):
        return (
            row.amount_assessed > 0,
            "has assessed amount greater than 0"
            if row.amount_assessed > 0
            else "has assessed amount less than or equal to 0",
        )

    # if assessed amount is not available, use amount paid
    if pd.notna(row.amount_paid):
        return (
            row.amount_paid > 0,
            "has no assessed amount, but has a paid amount greater than 0"
            if row.amount_paid > 0
            else "has pno assessed amount, but has a paid amount less than or equal to 0",
        )

    # if state did provide assessed amount, but it is null for this row, then
    # consider the case not completed
    if (
        row.state_name in coverage_df.query("cases_with_amount_assessed > 0").index
        or row.state_name in coverage_df.query("cases_with_amount_paid > 0").index
    ):
        return (False, "has no assessed amount, but state provided assessed amount")

    # at this point you cannot determine the case is closed so return null
    return (
        np.NaN,
        "closed case with no assessed amount and no paid amount "
        "in a state that provided neither",
    )


if __name__ == "__main__":
    df = pd.read_csv(sys.argv[1])
    coverage = get_coverage_df(df)

    df = (
        df.assign(
            case_completed=lambda df: df.progress_apply(
                determine_case_completion, coverage_df=coverage, axis=1
            )
        )
        .assign(
            case_completed_reason=lambda df: df.progress_apply(
                lambda row: row.case_completed[1], axis=1
            )
        )
        .assign(
            case_completed=lambda df: df.progress_apply(
                lambda row: row.case_completed[0], axis=1
            )
        )
    )

    df.to_csv(sys.argv[2], index=False)
