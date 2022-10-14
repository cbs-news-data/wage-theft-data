GENERATED_FILES: output/south_carolina.csv

.PHONY: all cleanup

.INTERMEDIATE: output/cases.csv output/amounts.csv

all: $(GENERATED_FILES)

output/south_carolina.csv: \
		output/cases.csv \
		output/amounts.csv \
		src/normalize_data.py
	csvjoin -c 1 --left $(wordlist 1, 2, $^) | \
	python src/normalize_data.py "South Carolina" \
		--date_opened Received \
		--employer_name Respondent \
		--date_closed Closed \
		--amount_assessed "Wage Due Amt." \
		--amount_paid "Amt. Paid" \
		--violation_category "Type Citation" \
		> $@

output/cases.csv: input/Copy_of_FOIA_Request.xlsx
	in2csv --sheet Sheet1 $< > $@

output/amounts.csv: input/Copy_of_FOIA_Request.xlsx
	in2csv --sheet Sheet2 $< > $@

cleanup:
	rm -f $(GENERATED_FILES)