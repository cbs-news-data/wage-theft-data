GENERATED_FILES: output/wisconsin.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

output/wisconsin.csv: \
		input/Responsive_record.xlsx \
		src/normalize_data.py \
		hand/converters_violation_category.yaml
	in2csv $< | \
	python src/normalize_data.py Wisconsin \
		--date_opened "Received Date" \
		--violation_category "Category Description" \
		--employer_name Respondent \
		--amount_assessed "Amount Assessed" \
		--amount_paid "Payment Received" \
		--case_status "Case Status" \
		> $@

cleanup:
	rm -f output/*