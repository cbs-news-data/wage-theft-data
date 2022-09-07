"""
The data contains comma-separated citation descriptions.
Explodes them and assigns a sequence ID
"""

import sys
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
    .to_csv(index=False, line_terminator="\n")
)
