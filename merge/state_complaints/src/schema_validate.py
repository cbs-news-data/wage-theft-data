"""applies the schema to the merged data"""

import sys
import pandas as pd
from normalize_data import SCHEMA


print(
    pd.read_csv(sys.stdin, parse_dates=["date_opened", "date_closed", "date_paid"])
    .pipe(SCHEMA.validate)
    .to_csv(index=False)
)
