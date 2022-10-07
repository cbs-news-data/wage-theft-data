"""
This data contains "final wages" and "outstanding wages" columns, but no "amount paid" column
"""

import sys
import pandas as pd

print(
    pd.read_csv(sys.stdin)
    .assign(
        amount_paid=lambda df: df.apply(
            lambda row: row["Final Wages"] - row["Outstanding Wages"]
            if row["Outstanding Wages"] > 0
            else row["Final Wages"],
            axis=1,
        )
    )
    .to_csv(index=False)
)
