GENERATED_FILES: output/new_york.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

output/new_york.csv: \
		input/new_york.xlsx \
		src/explode_claim_types.py \
		src/normalize_data.py
	in2csv $< | \
	python src/explode_claim_types.py | \
	python src/normalize_data.py "New York" \
		--date_opened CASE_DATE \
		--employer_name "Account Name" \
		--employer_dba_name "Account DBA name" \
		--violation_category claim_type \
		--amount_claimed "Claim Amount" \
		--amount_assessed "Amount Assessed" \
		--amount_paid "Deposits" \
		> $@

cleanup:
	rm -f output/*