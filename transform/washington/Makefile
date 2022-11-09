GENERATED_FILES: output/washington.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

output/washington.csv: \
		input/146714.xlsx \
		src/normalize_data.py \
		hand/converters_violation_category.yaml
	in2csv --skip-lines 4 $< | \
	python src/normalize_data.py Washington \
		--employer_name Employer \
		--employer_city "ER Local City" \
		--date_opened "File Date" \
		--date_closed "Closed Date" \
		--amount_claimed "Amount Claimed" \
		--amount_assessed "Amount Assessed" \
		--violation_category "Violation Type" \
		> $@

cleanup:
	rm -f output/*