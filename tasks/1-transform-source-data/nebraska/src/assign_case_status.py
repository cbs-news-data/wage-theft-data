"""assigns case status field based on boolean fields"""

import sys
import pandas as pd


def assign_case_status(df: pd.DataFrame) -> pd.DataFrame:
    """assigns case status field based on boolean fields"""

    def assign_row_status(row: pd.Series) -> str:
        """assigns case status field based on boolean fields"""

        if row["Affirmed"] is True:
            return "affirmed"
        if row["Appeal"] is True:
            if pd.notna(row["Hearing Date"]):
                return "overturned"
            return "pending appeal"

        if row["Withdrawn"] is True:
            return "withdrawn"

        # this only contains citations to return 'closed'
        return "closed"

    df["case_status"] = df.apply(assign_row_status, axis=1)
    return df


if __name__ == "__main__":
    pd.read_csv(sys.stdin).pipe(assign_case_status).to_csv(sys.stdout, index=False)
