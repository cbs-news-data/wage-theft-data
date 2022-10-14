GENERATED_FILES: output/indiana.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

output/indiana.csv: \
		input/2019-2022.xlsx \
		input/Copy_of_Wage_Claim_APRA_2010-2019.xlsx \
		src/assign_amount_assessed.py \
		src/normalize_data.py

	in2csv $< | \
		python src/assign_amount_assessed.py | \
				python src/normalize_data.py Indiana \
				--employer_name "Employer Name" \
				--date_opened "Date Received" \
				--amount_claimed "Amount of Claim" \
				--amount_assessed amount_assessed \
				--amount_paid "Net Paid" | \
					awk "NR > 1" >> $@
	
	in2csv $(word 2, $^) | \
			python src/assign_amount_assessed.py | \
				python src/normalize_data.py Indiana \
				--employer_name Employer \
				--date_opened "ClaimReceived" \
				--date_paid "Date Paid" \
				--amount_claimed "Amount Claimed" \
				--amount_assessed amount_assessed \
				--amount_paid "Net Paid" \
				> $@

cleanup:
	rm -f $(GENERATED_FILES)