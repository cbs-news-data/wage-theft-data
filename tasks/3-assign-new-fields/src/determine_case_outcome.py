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

tqdm.pandas(desc="determine_case_outcome")

OPEN_OR_INCOMPLETE_STATUSES = [
    "open",
    "dismissed",
    "withdrawn",
    "pending appeal",
    "overturned",
    "amount exceeds statutory limit",
]

FINAL_CASE_STATUSES = ["pending enforcement", "affirmed"]

INDETERMINATE_CASE_STATUSES = ["closed"]

# states that I have confirmed contain only completed cases
ALWAYS_CLOSED_STATES = ["Texas"]


def determine_case_outcome(row: pd.Series, coverage_df: pd.DataFrame) -> bool:
    """determine whether the case was completed  and was not denied/withdrawn/dismissed/etc.

    Args:
        row (pd.Series): row of dataframe to evaluate
        coverage_df (pd.DataFrame): dataframe of data for each state

    Returns:
        bool: True if case is completed, False otherwise
    """
    # make sure the case status is in one of the above
    if (
        pd.notna(row.case_status)
        and row.case_status
        not in OPEN_OR_INCOMPLETE_STATUSES
        + FINAL_CASE_STATUSES
        + INDETERMINATE_CASE_STATUSES
    ):
        raise ValueError(f"unknown case status: {row.case_status}")

    # if the state is in the list of states that always have closed cases, then
    # return True
    if row.state_name in ALWAYS_CLOSED_STATES:
        return (True, "state only provided closed cases")

    # if the case has any of the following statuses, it is not completed
    if row.case_status in OPEN_OR_INCOMPLETE_STATUSES:
        return (False, "has open or incomplete case status")

    # any of these are always considered completed, even if the record
    # is missing amounts
    if row.case_status in FINAL_CASE_STATUSES:
        return (True, "has final case status")

    # if the case is paid, it is always considered completed
    if pd.notna(row.amount_paid):
        return (True, "has amount paid")

    # if the case status is null OR "closed", use the amount fields to determine
    # whether it is closed or not
    if pd.isna(row.case_status) or row.case_status in INDETERMINATE_CASE_STATUSES:
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
    # if the state provided no amounts, assume the case is not completed
    if (
        pd.isna(row.amount_claimed)
        and pd.isna(row.amount_assessed)
        and pd.isna(row.amount_paid)
    ):
        return (False, "has no amount values")

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
    df = pd.read_csv(sys.stdin)
    coverage = get_coverage_df(df)

    df = (
        df.assign(
            case_decided_in_favor_of_claimant=lambda df: df.progress_apply(
                determine_case_outcome, coverage_df=coverage, axis=1
            )
        )
        .assign(
            case_decided_in_favor_of_claimant_reason=lambda df: df.progress_apply(
                lambda row: row.case_decided_in_favor_of_claimant[1], axis=1
            )
        )
        .assign(
            case_decided_in_favor_of_claimant=lambda df: df.progress_apply(
                lambda row: row.case_decided_in_favor_of_claimant[0], axis=1
            )
        )
    )

    print(df.to_csv(index=False))
