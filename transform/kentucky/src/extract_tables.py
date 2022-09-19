"""extracts tables from pdf"""

import sys
import pandas as pd
import pdfplumber
from tqdm import tqdm

AMOUNT_PAT = r"(\$?[\d\.,]+)"

columns = None
dfs = []
with pdfplumber.open(sys.argv[1]) as pdf:
    for page in tqdm(pdf.pages):
        table = pdf.pages[0].extract_table(
            {
                "vertical_strategy": "text",
                "horizontal_strategy": "text",
            }
        )
        if columns is None:
            columns = table[0]

        df = pd.DataFrame(table[1:], columns=columns)

        # combine columns that were split
        LAST_NONNULL_COL = None
        for col_index, _ in enumerate(df.columns):
            col = df.columns[col_index]
            if col != "":
                LAST_NONNULL_COL = col

            if col == "":
                df[LAST_NONNULL_COL] = df[LAST_NONNULL_COL] + df.iloc[:, col_index]
        df = df[[c for c in df.columns if c != ""]]

        # split columns that were combined
        df = (
            df.assign(
                amount=lambda df: df["Amountaction_taken"].str.extract(AMOUNT_PAT)
            )
            .assign(
                action_taken=lambda df: df["Amountaction_taken"].str.replace(
                    AMOUNT_PAT, "", regex=True
                )
            )
            .assign(
                city=lambda df: df["City State"].str.replace(
                    r"\s*[A-Z]{2}\s*$", "", regex=True
                )
            )
            .assign(state=lambda df: df["City State"].str.extract(r"\s*([A-Z]{2})\s*$"))
            .drop(["Amountaction_taken", "City State"], axis=1)
        )
        dfs.append(df)

df = pd.concat(dfs)
for col in df.columns:
    if df[col].dtype == "object":
        df[col] = df[col].str.normalize("NFKC")

print(df.to_csv(index=False, line_terminator="\n", encoding="utf-8"))
