"""reads table from html to csv"""

import sys
import pandas as pd

print(pd.read_html(sys.argv[1])[0].to_csv(index=False))
