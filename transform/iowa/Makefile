GENERATED_FILES: output/iowa.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

output/iowa.csv: \
		input/iowa.xlsx \
		src/normalize_data.py
	in2csv $< | \
	python src/normalize_data.py Iowa \
		--employer_name "Account DBA Name" \
		--date_opened "Date Received" \
		--amount_claimed "Claim Amount" \
		--amount_assessed "Amount Assessed" \
		--amount_paid "Amount Received" \
		> $@

cleanup:
	rm -f output/*