"""
some of the cells are merged downward, probably because of how they exported the data.
this script fills in the missing values by copying the value from the cell above.
"""

import sys
import pandas as pd


def ffill_selected_cols(df, cols):
    """ffills only selected columns"""
    df[cols] = df[cols].fillna(method="ffill", axis=0)
    return df


print(
    pd.read_csv(sys.stdin)
    .pipe(
        ffill_selected_cols,
        [
            "Case Number",
            "Received Date",
            "Case Status",
            "Category Description",
            "Respondent",
            "DBA Name",
        ],
    )
    .to_csv(index=False)
)
