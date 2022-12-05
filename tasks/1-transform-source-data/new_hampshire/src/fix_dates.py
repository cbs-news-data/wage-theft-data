"""combines date columns"""

import sys
import pandas as pd

print(
    pd.read_csv(sys.stdin)
    .assign(
        received_date=lambda df: df["Received Year"]
        + df["Received Month"]
        + df["Received Day"]
    )
    .assign(received_date=lambda df: pd.to_datetime(df.received_date))
    .drop(["Received Year", "Received Month", "Received Day"], axis=1)
    .to_csv(index=False)
)
