"""normalizes all datasets from various sources"""

import argparse
import os
import re
import sys
from typing import Union
import uuid
import pandas as pd
import pandera as pa
import numpy as np
from tqdm import tqdm
import yaml

SCHEMA = pa.DataFrameSchema(
    columns={
        "uuid": pa.Column(
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
        "citation_sequence": pa.Column(
            dtype=float,
            nullable=True,
            unique=False,
        ),
        "employer_name": pa.Column(
            dtype=str,
            nullable=False,
            unique=False,
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
                    "minimum_wage",
                    "tips",
                    "deductions",
                    "overtime",
                    "breaks",
                    "unpaid_wages",
                    "time_off",
                    "sick_time",
                    "other",
                ],
            ),
            coerce=True,
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
        "paid_in_full": pa.Column(
            dtype="object",
            nullable=True,
            unique=False,
            coerce=True,
            checks=pa.Check.isin([True, False]),
        ),
        "appeal_filed": pa.Column(
            dtype="object",
            nullable=True,
            unique=False,
            coerce=True,
            checks=pa.Check.isin([True, False]),
        ),
        "lien_issued": pa.Column(
            dtype="object",
            nullable=True,
            unique=False,
            coerce=True,
            checks=pa.Check.isin([True, False]),
        ),
        "specific_intent": pa.Column(
            dtype="object",
            nullable=True,
            unique=False,
            coerce=True,
            checks=pa.Check.isin([True, False]),
        ),
    },
)


def schema_col_is_datetime(column: pa.Column) -> bool:
    """checks whether the pandera.Column object has a dtype of "datetime64[ns]"

    Args:
        column (pandera.Column): Column object to check

    Returns:
        bool: True if datetime, False if not
    """
    return column.dtype.type == "datetime64[ns]"


def strtobool(val: any) -> bool:
    """Convert a string representation of truth to True or False"""
    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return True
    elif val in ("n", "no", "f", "false", "off", "0"):
        return False
    else:
        raise ValueError(f"invalid truth value {val}")


def parse_bool(val: any) -> Union[bool, any]:
    """parses boolean columns into bool objects or returns the original value

    Args:
        val (any): value to parse as bool

    Returns:
        Union[bool, any]: bool if conversion successful, original value if not
    """
    match val:
        case bool():
            return val
        case str():
            val = val.lower().strip()
            try:
                return bool(strtobool(val)) if len(val) > 0 else False
            except ValueError:
                return val
        case other:  # type: ignore # pylint: disable=unused-variable
            return val


def clean_amount(val: any) -> any:
    """attempts to clean amount values formatted as strings

    Args:
        val (any): value to clean

    Returns:
        Union[int, float]: if an amount value was found
        np.NaN if a string was provided that did not contain an amount
        any: the original value, if any type other than int, float, or str was provided
    """
    match val:
        # return numeric values
        case int() | float():
            if not isinstance(val, bool):
                return val

        # attempt to parse string values
        case str():
            # find all numbers
            amt_strings = [
                re.sub(r"[\,]", "", s)
                for s in re.findall(
                    r"^[\d\,\.]+(?:k|m)?|(?<=\s|\$)[\d\,\.]+(?:k|m)?",
                    val,
                )
            ]

            # if matches were found, parse them
            if len(amt_strings) > 0:
                # if a range was provided, use the smallest amount
                vals = []
                for string in amt_strings:

                    # convert the value to float
                    try:
                        amt = float(re.sub(r"(k|m)", "", string, flags=re.IGNORECASE))
                    except ValueError:
                        # if error, return NaN
                        return np.NaN

                    # if 1,000 or 1 million, convert the numbers
                    if "k" in string.lower():
                        amt = amt * 1000
                    elif "m" in string.lower():
                        amt = amt * 1_000_000
                    vals.append(amt)

                return min(amt_strings)

            # otherwise return NaN
            else:
                return np.NaN

    # return all other original values
    return val


CLEAN_FUNCTIONS = {
    "paid_in_full": parse_bool,
    "appeal_filed": parse_bool,
    "lien_issued": parse_bool,
    "specific_intent": parse_bool,
    "amount_claimed": clean_amount,
    "amount_assessed": clean_amount,
    "amount_paid": clean_amount,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "infile",
        type=argparse.FileType("r"),
        help="Optional path to an input csv file, otherwise reads from stdin",
        nargs="?",
        default=sys.stdin,
    )
    parser.add_argument(
        "state_name",
        type=str,
        help="The name of the agency that provided the data",
    )

    # dynamically add arguments for fields from schema
    IGNORE_COLS = ["uuid", "state_name", "citation_index"]
    for colname, pa_col in SCHEMA.columns.items():
        if colname in IGNORE_COLS:
            continue

        help_msg = f"the name of the column in infile to map to schema for column '{colname}'. "

        if len(pa_col.checks) > 0:
            for check in pa_col.checks:
                if "allowed_values" in check.statistics:
                    help_msg += "Must be one of: " + "\n".join(
                        [
                            str(v) if not isinstance(v, str) else f"'{v}'"
                            for v in check.statistics["allowed_values"]
                        ]
                    )

        parser.add_argument(
            f"--{colname}",
            type=str,
            help=help_msg,
        )

    args = parser.parse_args()

    # read input from file or stdin
    df = pd.read_csv(args.infile)

    # assign uuid and state fields
    df["uuid"] = df.apply(lambda _: str(uuid.uuid4()), axis=1)
    df["state_name"] = args.state_name

    # loop over all columns, apply cleaners and add if not present
    for dest_colname in list(vars(args))[2:]:
        source_colname = getattr(args, dest_colname)
        schema_col = SCHEMA.columns[dest_colname]

        # if the column name was provided in command line args,
        # get the source data and make transformations on it
        if source_colname is not None:
            # if a file called converters_{filename} is present, replace values from file)
            converter_path = f"hand/converters_{dest_colname}.yaml"
            if os.path.exists(converter_path):
                with open(converter_path, "r", encoding="utf-8") as converter_file:
                    converters = yaml.load(converter_file, Loader=yaml.CLoader)

                if not isinstance(converters, dict):
                    raise ValueError(
                        f"{converter_path} must be a dictionary, got {type(converters)}"
                    )

                # use source column name so cleaners can also be applied
                df[source_colname] = df[source_colname].replace(converters)

            # create a new column with the expected colname based on vals, optionally
            # applying a clean function to each value
            tqdm.pandas(leave=False, desc=dest_colname)
            df[dest_colname] = (
                df[source_colname].progress_apply(CLEAN_FUNCTIONS[dest_colname])
                if dest_colname in CLEAN_FUNCTIONS
                else df[source_colname]
                # if schema expects a datetime, convert values to datetime
                if not schema_col_is_datetime(schema_col)
                else pd.to_datetime(df[source_colname], errors="coerce")
            )

        else:
            # if the schema expects a datetime, create a null datetime field
            if schema_col_is_datetime(schema_col):
                df[dest_colname] = pd.to_datetime(np.NaN)
            else:
                df[dest_colname] = np.NaN

    print(
        SCHEMA.validate(df)[list(SCHEMA.columns.keys())]
        .set_index("uuid")
        .to_csv(line_terminator="\n")
    )
