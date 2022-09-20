"""drops violations that are not wage theft related"""

import sys
import pandas as pd
import yaml

with open(sys.argv[1], "r", encoding="utf-8") as yaml_file:
    keep_viols = list(yaml.load(yaml_file, Loader=yaml.CLoader).keys())

print(
    pd.read_csv(sys.stdin)
    .pipe(lambda df: df[df["Violation Type"].isin(keep_viols)])
    .to_csv(index=False, line_terminator="\n")
)
