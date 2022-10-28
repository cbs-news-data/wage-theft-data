"""transforms california data from original report format to our standard format"""

import logging
import sys
import pandas as pd
from tqdm import tqdm

logging.basicConfig(
    filename="output/transform_ca_claims_data.log", level=logging.INFO, filemode="w"
)


def drop_header_rows(df):
    """drop header rows from the dataframe"""
    header_keyword = "DIR Case Name"
    if any(header_keyword in col for col in df.columns):
        return df

    header_row = df[df.iloc[:, 1].str.contains(header_keyword, na=False)].index[0]
    df = df.iloc[header_row:]
    # set first row as header row
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    df.columns.name = None
    df = df.reset_index(drop=True)
    return df


def cleanup_column_names(df):
    """replace all non-alphanumeric characters in column names with underscores"""
    df.columns = (
        df.columns.str.replace(r"[^a-zA-Z0-9]", " ", regex=True)
        .str.strip()
        .str.replace(r"\s+", "_", regex=True)
        .str.lower()
    )
    return df


def do_transformation(filename):
    """transforms california data from original report format to our standard format"""
    return (
        pd.read_excel(filename)
        .pipe(drop_header_rows)
        .pipe(cleanup_column_names)
        # delete all rows that contain "Subtotal" or "Total" in the dir_case_name column
        # or that contain any non-nan value in the second column
        .pipe(
            lambda df: df[
                (~df["dir_case_name"].str.contains("Subtotal|Total", na=False))
                & (df.iloc[:, 2].isna())
            ]
        )
        # drop completely empty columns
        .dropna(axis=1, how="all")
        # drop completely empty rows
        .dropna(axis=0, how="all")
        # ffill values
        # first need to ffill the case name
        .assign(dir_case_name=lambda df: df.dir_case_name.ffill())
        # then groupby the case number and ffill the rest of the values
        # this prevents values from being ffilled across different cases
        .pipe(
            lambda df: df[["dir_case_name"]].join(df.groupby("dir_case_name").ffill())
        )
        # group by identifying info and sum all other columns
        .groupby(
            [
                "dir_case_name",
                "account_name",
                "account_dba",
                "date_of_docket",
                "naics_code",
                "role",
                "oda_decision_date",
                "case_status",
            ],
            dropna=False,
        )
        .sum()
    )


if __name__ == "__main__":
    for i, file in tqdm(enumerate(sys.argv[1:])):
        header = i == 0
        try:
            do_transformation(file).to_csv(sys.stdout, index=True, header=header)
        except Exception as e:  # pylint: disable=broad-except
            logging.error("Error processing file %s: %s", file, e)
            raise e
        else:
            logging.info("Processed file %s", file)
