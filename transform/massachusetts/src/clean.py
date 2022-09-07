"""
The data contains comma-separated citation descriptions.
Explodes them and assigns a sequence ID
"""

import sys
import numpy as np
import pandas as pd

print(
    pd.read_excel(sys.argv[1])
    # explode violation descriptions and assign uniqiue IDs
    .assign(
        violation_description=lambda df: df["Violation Description"].apply(
            lambda val: [v.strip() for v in val.split(",")]
            if isinstance(val, str)
            else val
        )
    )
    .explode("violation_description")
    .reset_index()
    .assign(index=lambda df: df.groupby("index").cumcount().add(1).astype(float))
    .rename(columns={"index": "citation_sequence"})
    .assign(violation_description=lambda df: df.violation_description.str.strip())
    # remove state abbreviation from city, state field
    .assign(
        city=lambda df: df["City, State"]
        .str.replace(r"\,\s[A-Z]{2}$", "", regex=True)
        .str.strip()
    )
    # drop vacated records
    .pipe(lambda df: df[df["Paid In Full"] != "Vacated"])
    # drop missing employer names
    .pipe(lambda df: df[df["Employer"].notna()])
    # drop missing violation categories
    .query("violation_description.notna()")
    # convert "yes" subtypes in appeal filed to "yes"
    .assign(
        **{
            "Appeal Filed": lambda df: df["Appeal Filed"].apply(
                lambda val: "Yes" if "yes" in val.lower() else val
            )
        }
    )
    # drop "n/a" values in specific intent field and fill missing values with "No"
    .assign(
        **{
            "With Specific Intent": lambda df: df["With Specific Intent"].apply(
                lambda val: np.NaN
                if val == "N/A"
                else val
                if pd.notna(val) and val != ""
                else "No"
            )
        }
    )
    .to_csv(index=False, line_terminator="\n")
)

