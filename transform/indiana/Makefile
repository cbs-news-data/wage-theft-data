GENERATED_FILES: output/indiana.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

output/indiana.csv: \
		input/indiana.xlsx \
		input/indiana1.xlsx \
		src/assign_amount_assessed.py \
		src/normalize_data.py
	in2csv input/indiana.xlsx | \
		csvgrep -c Employer -r "^$$" --invert-match | \
			python src/assign_amount_assessed.py | \
				python src/normalize_data.py Indiana \
				--employer_name Employer \
				--date_opened "ClaimReceived" \
				--date_paid "Date Paid" \
				--amount_claimed "Amount Claimed" \
				--amount_assessed amount_assessed \
				--amount_paid "Net Paid" \
				> $@

	in2csv input/indiana1.xlsx | \
		python src/assign_amount_assessed.py | \
			csvgrep -c "Employer Name" -r "^$$" --invert-match | \
				python src/normalize_data.py Indiana \
				--employer_name "Employer Name" \
				--date_opened "Date Received" \
				--amount_claimed "Amount of Claim" \
				--amount_assessed amount_assessed \
				--amount_paid "Net Paid" | \
					awk "NR > 1" >> $@

cleanup:
	rm -f output/*