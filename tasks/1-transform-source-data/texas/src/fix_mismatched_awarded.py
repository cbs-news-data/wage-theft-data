import sys
import pandas as pd
from constants import TX_PAID_STATUS_CODES


def fix_mismatched_awarded(row):
    if row["FK_VCMPLNT_STSCD"] in TX_PAID_STATUS_CODES:
        return "YES"
    return row["AWARDED"]


def main():
    df = pd.read_csv(sys.stdin)
    df["AWARDED"] = df.apply(fix_mismatched_awarded, axis=1)
    print(df.to_csv(index=False))


if __name__ == "__main__":
    main()
