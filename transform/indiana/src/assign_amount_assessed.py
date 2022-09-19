"""
assigns an "amount_assessed" field based on claim amount and wages owed determination

The data contains an "Amount Claimed" field and a "Wages owed" field but not an amount assessed.
Consider those with wages owed to be assessed the claim amount.
"""

import sys
import numpy as np
import pandas as pd

df = pd.read_csv(sys.stdin)

if "Amount Claimed" in df.columns:
    AMOUNT_FIELD = "Amount Claimed"
    OWED_FIELD = "Wages Owed?"
else:
    AMOUNT_FIELD = "Amount of Claim"
    OWED_FIELD = "Determination (Are Wages Owed)"

print(
    df.assign(
        amount_assessed=lambda df: df.apply(
            lambda row: row[AMOUNT_FIELD] if row[OWED_FIELD] == "Yes" else np.NaN,
            axis=1,
        )
    ).to_csv(index=False, line_terminator="\n")
)
