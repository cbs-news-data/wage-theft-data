"""cleans up case numbers so data can be joined"""

import sys
import pandas as pd


print(
    pd.read_csv(sys.stdin)
    .rename(columns=lambda col: "Case" if col.strip() == "Case" else col)
    .assign(Case=lambda df: df["Case"].str.strip().str.lower())
    .to_csv(index=False, lineterminator="\n")
)
