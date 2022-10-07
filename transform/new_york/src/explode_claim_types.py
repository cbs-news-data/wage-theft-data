"""claim types contain multiple values, explode them into separate rows"""

import sys
import pandas as pd

print(
    pd.read_csv(sys.stdin)
    .assign(claim_type=lambda df: df["Claim Types"].str.split(","))
    .explode("claim_type")
    .to_csv(index=False)
)
