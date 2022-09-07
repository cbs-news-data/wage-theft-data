"""normalizes all datasets from various sources"""

import argparse
from distutils.util import strtobool
import os
import sys
from typing import Union
import uuid
import pandas as pd
import pandera as pa
import numpy as np
from tqdm import tqdm
import yaml

tqdm.pandas()

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
                    "MontanaNebraska",
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
                    "PennsylvaniaRhode Island",
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
            dtype=str,
            nullable=True,
            unique=False,
        ),
        "employer_city": pa.Column(dtype=str, nullable=True, unique=False),
        "violation_category": pa.Column(
            dtype=str,
            nullable=False,
            unique=False,
            checks=pa.Check.isin(
                [
                    "minimum_wage",
                    "tips",
                    "overtime",
                    "breaks",
                    "unpaid_wages",
                    "time_off",
                    "sick_time",
                    "other",
                ],
            ),
        ),
        "date_opened": pa.Column(
            dtype="datetime64[ns]",
            nullable=False,
            unique=False,
        ),
        "amount_claimed": pa.Column(
            dtype=float,
            nullable=True,
            unique=False,
        ),
        "amount_assessed": pa.Column(
            dtype=float,
            nullable=False,
            unique=False,
        ),
        "amount_paid": pa.Column(
            dtype=float,
            nullable=True,
            unique=False,
        ),
        "paid_in_full": pa.Column(
            dtype=bool,
            nullable=True,
            unique=False,
        ),
        "appeal_filed": pa.Column(
            dtype=bool,
            nullable=True,
            unique=False,
        ),
        "lien_issued": pa.Column(
            dtype=bool,
            nullable=True,
            unique=False,
        ),
        "specific_intent": pa.Column(
            dtype=bool,
            nullable=True,
            unique=False,
        ),
    },
)


def schema_col_is_datetime(schema_col: pa.Column) -> bool:
    """checks whether the pandera.Column object has a dtype of "datetime64[ns]"

    Args:
        schema_col (pandera.Column): Column object to check

    Returns:
        bool: True if datetime, False if not
    """
    return schema_col.dtype.type == "datetime64[ns]"


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
        case other:
            return val


CLEAN_FUNCTIONS = {
    "paid_in_full": parse_bool,
    "appeal_filed": parse_bool,
    "lien_issued": parse_bool,
    "specific_intent": parse_bool,
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
        parser.add_argument(
            f"--{colname}",
            type=str,
            help="the name of the column in infile to map to schema",
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
                with open(converter_path, "r") as converter_file:
                    converters = yaml.load(converter_file, Loader=yaml.CLoader)

                if not isinstance(converters, dict):
                    raise ValueError(
                        f"{converter_path} must be a dictionary, got {type(converters)}"
                    )

                # use source column name so cleaners can also be applied
                df[source_colname] = df[source_colname].replace(converters)

            # create a new column with the expected colname based on vals, optionally
            # applying a clean function to each value
            df[dest_colname] = (
                df[source_colname].progress_apply(CLEAN_FUNCTIONS[dest_colname])
                if dest_colname in CLEAN_FUNCTIONS
                else df[source_colname]
                # if schema expects a datetime, convert values to datetime
                if not schema_col_is_datetime(schema_col)
                else pd.to_datetime(df[source_colname])
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
