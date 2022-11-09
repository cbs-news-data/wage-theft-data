"""
Some data rows labeled as "open" have a paid amount. This script will replace the case status with
null if there is a paid amount, even if the status indicates that the case is still open.
"""

import sys
import numpy as np
import pandas as pd

STATUS_COL = sys.argv[1]
PAID_COL = sys.argv[2]

print(
    pd.read_csv(sys.stdin)
    .assign(
        **{STATUS_COL: lambda df: df[STATUS_COL].where(df[PAID_COL].isna(), np.NaN)}
    )
    .to_csv(index=False)
)
