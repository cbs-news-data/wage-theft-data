"""dedupes the data on case ID"""

import sys
import pandas as pd

print(
    pd.read_csv(sys.stdin).drop_duplicates(subset="WAGE_CLAIM_ID").to_csv(index=False)
)
