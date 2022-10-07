GENERATED_FILES: output/new_hampshire.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

output/new_hampshire.csv: \
		input/new_hampshire.xlsx \
		src/fix_dates.py \
		src/normalize_data.py
	in2csv --no-inference $< | python src/fix_dates.py | \
	python src/normalize_data.py "New Hampshire" \
		--employer_name "Claim Against Employer" \
		--date_opened received_date \
		--amount_claimed "Amount Requested" \
		--amount_paid "Amount Collected" \
		> $@

cleanup:
	rm -f output/*