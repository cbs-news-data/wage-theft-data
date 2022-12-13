"""generates reports for a given geographic area"""

import argparse
import sys
from humanize import fractional
import jinja2
import pandas as pd
from shared_functions import append_texas_amounts


def get_human_readable(num):
    """returns a human-readable number"""
    if num >= 1:
        return round(num)

    return fractional(round(num, 1))


class MarkdownReportGenerator:
    """Class for generating reports"""

    def __init__(self, wt_file, state_desc_file, template_file, state_name):
        self.state_name = state_name
        # read in wage theft data
        self.wt_df = (
            pd.read_csv(
                wt_file,
                low_memory=False,
                parse_dates=["date_opened", "date_closed", "date_paid"],
            )
            # keep only 1 row per case. some rows contain multiple violation
            # categories, but this script will not address those.
            .drop_duplicates(subset="case_uuid", keep="first")
        )
        # read in state characteristics
        self.ddf = pd.read_csv(state_desc_file)

        # filter the appropriate state(s)
        unique_states = self.wt_df.state_name.unique()
        pretty_state_name = self.state_name.replace("_", " ").title()
        if pretty_state_name in unique_states:
            self.wt_df = self.wt_df.query("state_name == @pretty_state_name")
            self.ddf = self.ddf.query("state_name == @pretty_state_name")
        else:
            raise ValueError(
                f"Invalid state name: '{self.state_name}'. "
                f"Must be one of {unique_states} or 'all'"
            )

        self.data = {"state_name": self.state_name.replace("_", " ").title()}
        self.preprocess_data()

        # handle separate amount files if applicable
        if self.state_name.lower() == "texas":
            self.amounts_df = append_texas_amounts(
                self.wt_df, filename="input/texas_amounts.csv.gz"
            )
        else:
            self.amounts_df = self.wt_df

        # bail if there are no amounts
        if (
            len(
                self.amounts_df.query(
                    "amount_claimed.notna() | amount_assessed.notna() | amount_paid.notna()"
                )
            )
            == 0
        ):
            print(f"No case amount data for {self.state_name}. Skipping.")
            sys.exit(0)

        self.get_data()

        # load template
        self.template = jinja2.Environment(
            loader=jinja2.FileSystemLoader("./"), undefined=jinja2.StrictUndefined
        ).get_template(template_file)

    def preprocess_data(self):
        """preprocesses data for the report"""
        # filter for cases decided in favor of claimant
        claimant_won = self.wt_df.query("case_decided_in_favor_of_claimant == True")
        # if no cases decided in favor of claimant, use all cases and add disclaimer to self.data
        if len(claimant_won) == 0:
            claimant_won = self.wt_df
            self.data["no_cases_decided_in_favor_of_claimant"] = True
        else:
            self.data["no_cases_decided_in_favor_of_claimant"] = False
        self.wt_df = claimant_won

    def get_data(self):
        """populates all data for the report"""

        # handle states that don't have claim amounts
        if len(self.amounts_df.query("amount_claimed.notna()")) == 0:
            self.data["state_has_claim_amount"] = False
            self.data["case_amount_field"] = (
                "amount_assessed"
                if len(self.amounts_df.query("amount_assessed.notna()")) > 0
                else "amount_paid"
            )
        else:
            self.data["state_has_claim_amount"] = True
            self.data["case_amount_field"] = "amount_claimed"

        self.data["total_claims"] = len(self.wt_df)
        desc_row = self.ddf.iloc[0]
        self.data["statute_name"] = desc_row.statute_name
        self.data["agency_name"] = desc_row.agency_name

        # overall data info
        self.data["total_records"] = len(self.wt_df)

        # provide a human-readable rounded number of total_records
        self.data[
            "total_records_description"
        ] = f"approximately {round(self.data['total_records']):,}"
        self.data["min_date"] = (
            self.wt_df.date_opened.min()
            if len(self.wt_df.query("date_opened.notna()")) > 0
            else self.wt_df.date_closed.min()
            if len(self.wt_df.query("date_closed.notna()")) > 0
            else self.wt_df.date_paid.min()
        )
        self.data["total_case_amount"] = self.amounts_df.overall_case_amount.sum()
        self.data["median_case_amount"] = self.amounts_df.overall_case_amount.median()

        # contextualize the data
        self.data["state_median_weekly_income"] = desc_row.median_weekly_income
        self.data["equivalent_weeks_of_income"] = get_human_readable(
            self.data["median_case_amount"] / self.data["state_median_weekly_income"]
        )
        self.data["equivalent_rent_payments"] = get_human_readable(
            self.data["median_case_amount"] / desc_row.fair_market_rent_3br
        )
        self.data["equivalent_mortgage_payments"] = get_human_readable(
            self.data["median_case_amount"] / desc_row.median_monthly_mortgage
        )
        self.data["equivalent_family_weekly_grocery_budget"] = get_human_readable(
            self.data["median_case_amount"] / desc_row.low_cost_plan_grocery_estimate
        )

        # paid amounts
        self.data["state_has_paid_amount"] = (
            len(self.amounts_df.query("amount_paid.notna()")) > 0
        )
        if self.data["state_has_paid_amount"]:
            self.data["pct_cases_paid"] = len(
                self.amounts_df.query("amount_paid > 0")
            ) / len(self.amounts_df)
            self.data["total_amount_paid"] = self.amounts_df.amount_paid.sum()
            self.data["median_amount_paid"] = self.amounts_df.amount_paid.median()
            self.data["pct_cases_paid_full"] = len(
                self.amounts_df.assign(
                    amount_claimed_or_assessed=lambda df: df.apply(
                        lambda row: row.amount_assessed
                        if pd.notna(row.amount_assessed)
                        else row.amount_claimed,
                        axis=1,
                    )
                ).query("amount_paid != 0 & amount_paid == amount_claimed_or_assessed")
            ) / len(self.amounts_df)

        # case duration
        self.data["state_has_case_duration"] = (
            len(
                self.wt_df.query(
                    "date_opened.notna() & (date_closed.notna() | date_paid.notna())"
                )
            )
            > 0
        )
        self.data["median_case_duration"] = self.wt_df.case_duration.median()
        self.data["case_durations_dict"] = (
            self.wt_df.groupby(
                pd.cut(self.wt_df.case_duration, [0, 30, 60, 90, 180, 360, 720, 999999])
            )
            .size()
            .to_frame("total_cases")
            .rename(
                index={
                    pd.Interval(0, 30, closed="right"): "0-30 days",
                    pd.Interval(30, 60, closed="right"): "30-60 days",
                    pd.Interval(60, 90, closed="right"): "60-90 days",
                    pd.Interval(90, 180, closed="right"): "90-180 days",
                    pd.Interval(180, 360, closed="right"): "180-360 days",
                    pd.Interval(360, 720, closed="right"): "360-720 days",
                    pd.Interval(720, 999999, closed="right"): "720+ days",
                }
            )
            .assign(pct_of_total_cases=lambda df: df.total_cases / df.total_cases.sum())
            .to_dict(orient="index")
        )
        self.data["case_amounts_dict"] = (
            self.amounts_df.groupby(
                pd.cut(
                    self.amounts_df.overall_case_amount,
                    [0, 100, 500, 1000, 2000, 5000, 10000, 999999],
                )
            )
            .size()
            .to_frame("total_cases")
            .rename(
                index={
                    pd.Interval(0, 100, closed="right"): "$0-$100",
                    pd.Interval(100, 500, closed="right"): "$100-$500",
                    pd.Interval(500, 1000, closed="right"): "$500-$1,000",
                    pd.Interval(1000, 2000, closed="right"): "$1,000-$2,000",
                    pd.Interval(2000, 5000, closed="right"): "$2,000-$5,000",
                    pd.Interval(5000, 10000, closed="right"): "$5,000-$10,000",
                    pd.Interval(10000, 999999, closed="right"): "$10,000+",
                }
            )
            .assign(pct_of_total_cases=lambda df: df.total_cases / df.total_cases.sum())
            .to_dict(orient="index")
        )

    def get_report_text(self):
        """generates the report"""
        return self.template.render(**self.data)


def main():
    """main function"""
    # parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("wage_theft_file", help="wage theft data file")
    parser.add_argument("state_desc_file", help="state characteristics file")
    parser.add_argument("template_file", help="template file")
    parser.add_argument("state_name", help="state name")
    args = parser.parse_args()

    # generate the report
    report_generator = MarkdownReportGenerator(
        args.wage_theft_file,
        args.state_desc_file,
        args.template_file,
        args.state_name,
    )
    print(report_generator.get_report_text())


if __name__ == "__main__":
    main()
