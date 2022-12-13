GENERATED_FILES: output/wisconsin.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

include ../../../Makefile

output/wisconsin.csv: \
		input/Responsive_record.xlsx \
		$(SHARED_DIR)/src/normalize_data.py \
		src/ffill_rows.py \
		$(SHARED_DIR)/src/fix_case_status.py \
		$(wildcard hand/*)
	in2csv $< | \
	python src/ffill_rows.py | \
	python $(SHARED_DIR)/src/fix_case_status.py "Case Status" "Payment Received" | \
	python $(SHARED_DIR)/src/normalize_data.py Wisconsin \
		--date_opened "Received Date" \
		--violation_category "Category Description" \
		--employer_name Respondent \
		--amount_assessed "Amount Assessed" \
		--amount_paid "Payment Received" \
		--case_status "Case Status" \
	> $@

cleanup:
	rm -f output/*