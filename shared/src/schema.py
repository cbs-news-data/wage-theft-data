"""contains the schema for the data"""

import pandas as pd
import pandera as pa

# checks
def check_no_status_amount_mismatch(dataframe: pd.DataFrame) -> bool:
    """
    checks that there are no rows where case statuses show 'open'
    but payment amounts are non-null and non-zero
    """

    return not (
        (dataframe.case_status == "open")
        & ((dataframe.amount_paid.notna()) & (dataframe.amount_paid != 0))
    ).any()


SCHEMA = pa.DataFrameSchema(
    columns={
        "case_uuid": pa.Column(
            dtype=str,
            nullable=False,
            unique=False,
        ),
        "violation_uuid": pa.Column(
            dtype=str,
            nullable=False,
            unique=True,
        ),
        "state_name": pa.Column(
            dtype=str,
            nullable=False,
            unique=False,
            checks=pa.Check.isin(
                [
                    "Alabama",
                    "Alaska",
                    "Arizona",
                    "Arkansas",
                    "California",
                    "Colorado",
                    "Connecticut",
                    "Delaware",
                    "Florida",
                    "Georgia",
                    "Hawaii",
                    "Idaho",
                    "Illinois",
                    "Indiana",
                    "Iowa",
                    "Kansas",
                    "Kentucky",
                    "Louisiana",
                    "Maine",
                    "Maryland",
                    "Massachusetts",
                    "Michigan",
                    "Minnesota",
                    "Mississippi",
                    "Missouri",
                    "Montana",
                    "Nebraska",
                    "Nevada",
                    "New Hampshire",
                    "New Jersey",
                    "New Mexico",
                    "New York",
                    "North Carolina",
                    "North Dakota",
                    "Ohio",
                    "Oklahoma",
                    "Oregon",
                    "Pennsylvania",
                    "Rhode Island",
                    "South Carolina",
                    "South Dakota",
                    "Tennessee",
                    "Texas",
                    "Utah",
                    "Vermont",
                    "Virginia",
                    "Washington",
                    "West Virginia",
                    "Wisconsin",
                    "Wyoming",
                ]
            ),
        ),
        "employer_name": pa.Column(
            dtype=str, nullable=True, unique=False, required=True, coerce=True
        ),
        "employer_dba_name": pa.Column(
            dtype=str, nullable=True, unique=False, coerce=True
        ),
        "employer_city": pa.Column(dtype=str, nullable=True, unique=False, coerce=True),
        "violation_category": pa.Column(
            dtype=str,
            nullable=True,
            unique=False,
            checks=pa.Check.isin(
                [
                    "minimum wage",
                    "prevailing wage",
                    "tips",
                    "deductions",
                    "overtime",
                    "breaks",
                    "unpaid wages",
                    "benefits",
                ]
            ),
            coerce=True,
        ),
        "case_status": pa.Column(
            dtype=str,
            nullable=True,
            unique=False,
            coerce=True,
            checks=pa.Check.isin(
                [
                    "paid",
                    "claimant_won",
                    "closed",
                    "open",
                    "dismissed",
                    "withdrawn",
                    "pending enforcement",
                    "pending appeal",
                    "amount exceeds statutory limit",
                    "affirmed",
                    "overturned",
                ]
            ),
        ),
        "date_opened": pa.Column(
            dtype="datetime64[ns]",
            nullable=True,
            unique=False,
        ),
        "date_paid": pa.Column(
            dtype="datetime64[ns]",
            nullable=True,
            unique=False,
        ),
        "date_closed": pa.Column(
            dtype="datetime64[ns]",
            nullable=True,
            unique=False,
        ),
        "amount_claimed": pa.Column(
            dtype=float,
            nullable=True,
            unique=False,
            coerce=True,
        ),
        "amount_assessed": pa.Column(
            dtype=float,
            nullable=True,
            unique=False,
            coerce=True,
        ),
        "amount_paid": pa.Column(
            dtype=float,
            nullable=True,
            unique=False,
            coerce=True,
        ),
    },
    checks=[
        pa.Check(
            check_no_status_amount_mismatch,
            error="case status and amount paid mismatch",
        ),
    ],
)
