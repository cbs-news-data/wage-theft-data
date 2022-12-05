"""
The data contains comma-separated citation descriptions.
Explodes them and assigns a sequence ID
"""

import sys
import numpy as np
import pandas as pd

print(
    pd.read_excel(sys.argv[1])
    # split city,state
    .assign(
        city=lambda df: df["City, State"]
        .str.replace(r"\,\s[A-Z]{2}$", "", regex=True)
        .str.strip()
    )
    # drop vacated records
    .pipe(lambda df: df[df["Paid In Full"] != "Vacated"])
    # drop missing employer names
    .pipe(lambda df: df[df["Employer"].notna()])
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
    ).to_csv(index=False, lineterminator="\n")
)
