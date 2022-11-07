"""normalizes all datasets from various sources"""

import argparse
import os
import re
import sys
from typing import Union, Any
import uuid
import pandas as pd
import pandera as pa
import numpy as np
from tqdm import tqdm
import yaml

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
        "citation_sequence": pa.Column(
            dtype=int,
            nullable=True,
            unique=False,
            coerce=True,
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
                    "minimum_wage",
                    "tips",
                    "deductions",
                    "overtime",
                    "breaks",
                    "unpaid_wages",
                    "time_off",
                    "sick_time",
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
                    "closed",
                    "open",
                    "dismissed",
                    "withdrawn",
                    "pending enforcement",
                    "awaiting payment",
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
)


def schema_col_is_datetime(column: pa.Column) -> bool:
    """checks whether the pandera.Column object has a dtype of "datetime64[ns]"

    Args:
        column (pandera.Column): Column object to check

    Returns:
        bool: True if datetime, False if not
    """
    return column.dtype.type == "datetime64[ns]"  # type: ignore


def strtobool(val: Any) -> bool:
    """Convert a string representation of truth to True or False"""
    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return True

    if val in ("n", "no", "f", "false", "off", "0"):
        return False

    raise ValueError(f"invalid truth value {val}")


def parse_bool(val: Any) -> Union[bool, Any]:
    """parses boolean columns into bool objects or returns the original value

    Args:
        val (Any): value to parse as bool

    Returns:
        Union[bool, Any]: bool if conversion successful, original value if not
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


def clean_amount(val: Any) -> Any:
    """attempts to clean amount values formatted as strings

    Args:
        val (Any): value to clean

    Returns:
        Union[int, float]: if an amount value was found
        np.NaN if a string was provided that did not contain an amount
        Any: the original value, if any type other than int, float, or str was provided
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
            return np.NaN

    # return all other original values
    return val


def explode_violations(
    dataframe: pd.DataFrame,
    violation_column: str,
    delim: str,
) -> pd.DataFrame:
    """
    Explodes violation descriptions and assigns a sequence ID

    Args:
        dataframe (pandas.DataFrame): dataframe with violation descriptions
        violation_column (str): name of column containing lists of violation descriptions

    Returns:
        pandas.DataFrame: dataframe with exploded violations
    """
    return (
        dataframe.assign(
            violation_category=lambda dataframe: dataframe[violation_column].apply(
                lambda val: [v.strip() for v in val.split(delim)]
                if isinstance(val, str)
                else val
            )
        )
        .explode("violation_category")
        .reset_index()
        .assign(
            index=lambda dataframe: dataframe.groupby("index")
            .cumcount()
            .add(1)
            .astype(float)
        )
        .rename(columns={"index": "citation_sequence"})
        .assign(
            violation_category=lambda dataframe: dataframe.violation_category.str.strip()
        )
    )


def validate_yaml_files() -> None:
    """validates all YAML files in the data directory"""
    if not os.path.exists("hand"):
        return

    for filename in os.listdir("hand"):
        if filename.endswith(".yaml"):
            assert any(
                col in filename for col in SCHEMA.columns
            ), f"YAML file {filename} does not contain any columns from the source dataframe"

            with open(f"hand/{filename}", "r", encoding="utf-8") as yaml_file:
                yaml.load(yaml_file, Loader=yaml.CLoader)


def replace_col_vals_from_yaml(
    dataframe: pd.DataFrame, existing_colname: str, schema_colname: str
) -> pd.DataFrame:
    """replaces values in a column based on a yaml file

    Args:
        dataframe (pandas.DataFrame): dataframe to replace values in
        existing_colname (str): name of column to replace values in
        schema_colname (str): name of column in schema, which should appear in the yaml filename

    Returns:
        pandas.DataFrame: dataframe with replaced values
    """
    # if a file called converters_{filename} is present, replace values from file)
    converter_path = f"hand/converters_{schema_colname}.yaml"
    if os.path.exists(converter_path):
        with open(converter_path, "r", encoding="utf-8") as converter_file:
            converters = yaml.load(converter_file, Loader=yaml.CLoader)

        if not isinstance(converters, dict):
            raise ValueError(
                f"{converter_path} must be a dictionary, got {type(converters)}"
            )

        # use source column name so cleaners can also be applied
        dataframe[existing_colname] = dataframe[existing_colname].replace(converters)

    return dataframe


def explicit_drop_replaced_values(
    dataframe: pd.DataFrame, col_name: str
) -> pd.DataFrame:
    """drops any values in column col_name that were replaced with 'drop'"""
    return dataframe[dataframe[col_name] != "drop"]


CLEAN_FUNCTIONS = {
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
    parser.add_argument(
        "--explode-violations",
        action="store_true",
        help="If provided, violation descriptions will be exploded into separate rows",
    )
    parser.add_argument(
        "--violations-delim",
        type=str,
        default=None,
        help="Delimiter for exploding violations. "
        "Must be provided if --explode-violations is provided.",
    )
    parser.add_argument(
        "--paid-in-full",
        type=str,
        default=None,
        help="boolean column denoting whether the amount was paid",
    )

    # dynamically add arguments for fields from schema
    IGNORE_ARGS = [
        "case_uuid",
        "violation_uuid",
        "state_name",
        "citation_sequence",
        "infile",
        "explode_violations",
        "violations_delim",
        "paid_in_full",
    ]
    for colname, pa_col in SCHEMA.columns.items():
        if colname in IGNORE_ARGS:
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

    # before doing anything else, validate that all yaml files in hand/ directory
    validate_yaml_files()

    # assign uuid and state fields
    df["case_uuid"] = df.apply(lambda _: str(uuid.uuid4()), axis=1)
    df["state_name"] = args.state_name

    # if "paid in full" argument was provided, use it to calculate amount paid
    if args.paid_in_full is not None:
        assert (
            args.amount_claimed is not None or args.amount_assessed is not None
        ), "amount claimed or amount assessed must be provided to calculate amount paid"
        df["amount_paid"] = df.apply(
            lambda row: row[
                args.amount_assessed
                if args.amount_assessed is not None
                else args.amount_claimed
            ]
            if row[args.paid_in_full]
            else 0,
            axis=1,
        )
        args.amount_paid = "amount_paid"

    # optionally explode violations
    if args.explode_violations:
        assert (
            args.violations_delim is not None
        ), "violations delim must be provided to explode violations"
        assert (
            args.violation_category is not None
        ), "violation category column must be provided to explode violations"
        df = explode_violations(df, args.violation_category, args.violations_delim)

        # if exploding violations, always use "violation_category" as the column name
        args.violation_category = "violation_category"

    # otherwise assign sequence number 1 to all rows
    else:
        df["citation_sequence"] = pd.Series(1, index=df.index).astype(int)

    # assign violation uuid now that violations have been exploded
    df["violation_uuid"] = df.apply(lambda _: str(uuid.uuid4()), axis=1)

    # loop over all columns, apply cleaners and add if not present
    for dest_colname in list(vars(args)):
        if dest_colname in IGNORE_ARGS:
            continue

        source_colname = getattr(args, dest_colname)
        schema_col = SCHEMA.columns[dest_colname]

        # if the column name was provided in command line args,
        # get the source data and make transformations on it
        if source_colname is not None:
            df = replace_col_vals_from_yaml(df, source_colname, dest_colname)

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

    # drop rows whose status or violation_category is 'drop'
    df = explicit_drop_replaced_values(df, "violation_category")
    df = explicit_drop_replaced_values(df, "case_status")
    # all other values will fail validation

    print(
        SCHEMA.validate(df)[list(SCHEMA.columns.keys())]
        .set_index(["case_uuid", "violation_uuid"])
        .to_csv(lineterminator="\n")
    )
