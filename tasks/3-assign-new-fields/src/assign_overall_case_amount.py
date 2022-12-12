"""
Because the data provided by the states was so inconsistent, it's not always clear how much money
was actually owed to the claimants in each case. In some cases, the states provided a single
amount that was owed to the claimant, but in other cases, they provided the amount the person 
claimed, the amount they assessed to be owed, and the amount they actually paid.

This script will assign a new field called `overall_case_amount` to each case with the aim of
providing a single number that represents the amount of money that was owed to the claimant.
"""

import sys
from typing import Union
import pandas as pd
from tqdm import tqdm

tqdm.pandas(desc="assign_overall_case_amount")

import logging

logging.basicConfig(
    level=logging.INFO, filename="assign_overall_case_amount.log", filemode="a"
)


def is_present_and_valid(numeric_value: Union[float, int]) -> bool:
    """
    Args:
        numeric_value (float): numeric value to evaluate

    Returns:
        bool: True if value is present and non-negative, False otherwise
    """
    return pd.notna(numeric_value) and float(numeric_value) >= 0


def assign_overall_case_amount(row: pd.Series, colnames=None) -> float:
    """
    Args:
        row (pd.Series): row of dataframe to evaluate
        colnames (dict, optional): dictionary of column names. If provided,
            uses these in place of default column names. Defaults to None.

    Returns:
        float: overall case amount
    """

    if colnames is not None:
        amount_claimed = row[colnames["amount_claimed"]]
        amount_assessed = row[colnames["amount_assessed"]]
        amount_paid = row[colnames["amount_paid"]]
    else:
        amount_claimed = row.amount_claimed
        amount_assessed = row.amount_assessed
        amount_paid = row.amount_paid

    # if assessed amount is available for this case, use that
    if is_present_and_valid(amount_assessed):
        return amount_assessed

    # if assessed amount is not available, use amount paid
    # only if it is non-zero
    if is_present_and_valid(amount_paid) and amount_paid > 0:
        return amount_paid

    # return the amount claimed. if it's nan, then the case doesn't have
    # an overall case amount. these will be treated as incomplete cases in
    # determine_case_outcome.py infer_case_status_from_amount
    return amount_claimed


if __name__ == "__main__":
    df = pd.read_csv(sys.stdin)
    COLS = {
        "amount_claimed": "amount_claimed",
        "amount_assessed": "amount_assessed",
        "amount_paid": "amount_paid",
    }
    if "--colnames" in sys.argv:
        COLS = sys.argv[sys.argv.index("--colnames") + 1]
        COLS = COLS.split(",")
        COLS = {
            "amount_claimed": COLS[0],
            "amount_assessed": COLS[1],
            "amount_paid": COLS[2],
        }

    # set dtype of all amount fields to float
    df = df.astype(
        {
            COLS["amount_claimed"]: "float",
            COLS["amount_assessed"]: "float",
            COLS["amount_paid"]: "float",
        }
    )

    logging.info(df)
    logging.info(df.columns)
    logging.info(COLS)

    df = df.assign(
        overall_case_amount=lambda df: df.progress_apply(
            assign_overall_case_amount, colnames=COLS, axis=1
        )
    )
    print(df.to_csv(index=False))
