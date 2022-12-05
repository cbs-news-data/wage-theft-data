GENERATED_FILES: output/south_carolina.csv

.PHONY: all cleanup

.INTERMEDIATE: output/cases.csv output/amounts.csv

all: $(GENERATED_FILES)

include ../../../Makefile

output/south_carolina.csv: \
		output/cases.csv \
		output/amounts.csv \
		$(PROCESSOR_DIR)/normalize_data.py \
		$(wildcard hand/*)
	csvjoin -c 1 --left $(wordlist 1, 2, $^) | \
	python $(PROCESSOR_DIR)/normalize_data.py "South Carolina" \
		--date_opened Received \
		--employer_name Respondent \
		--date_closed Closed \
		--amount_assessed "Wage Due Amt." \
		--amount_paid "Amt. Paid" \
		--violation_category "Type Citation" \
		--case_status Status \
		> $@

output/cases.csv: \
		input/Copy_of_FOIA_Request.xlsx \
		src/cleanup_case_numbers.py
	in2csv --sheet Sheet1 $< | python src/cleanup_case_numbers.py > $@

output/amounts.csv: \
		input/Copy_of_FOIA_Request.xlsx \
		src/cleanup_case_numbers.py
	in2csv --sheet Sheet2 $< | python src/cleanup_case_numbers.py > $@

cleanup:
	rm -f output/*