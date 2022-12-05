"""merges and flattens data split across multiple tables"""

import sys
import pandas as pd

MERGE_ARGS = {"on": "case_number", "how": "left"}


print(
    pd.read_csv(sys.argv[1])
    .merge(
        pd.read_csv(sys.argv[2]).drop_duplicates(subset="case_number", keep="first"),
        **MERGE_ARGS
    )
    .merge(
        pd.read_csv(sys.argv[3])
        .groupby("case_number")
        .employer_name.unique()
        .apply(",".join)
        .to_frame("employer_name"),
        **MERGE_ARGS
    )
    .to_csv(index=False)
)
