GENERATED_FILES: output/new_york.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

include ../../../Makefile

output/new_york.csv: \
		input/REC-0011772_Records_for_release.xlsx \
		$(SHARED_DIR)/src/normalize_data.py \
		$(wildcard hand/*)
	in2csv $< | \
	python $(SHARED_DIR)/src/normalize_data.py "New York" \
		--explode-violations \
		--violations-delim "," \
		--date_opened CASE_DATE \
		--employer_name "Account Name" \
		--employer_dba_name "Account DBA name" \
		--violation_category "Claim Types" \
		--amount_claimed "Claim Amount" \
		--amount_assessed "Amount Assessed" \
		--amount_paid "Deposits" \
		--case_status "Case Status" \
		> $@

cleanup:
	rm -f output/*