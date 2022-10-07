GENERATED_FILES: output/new_jersey.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

output/new_jersey.csv: \
		input/new_jersey.xlsx \
		src/add_amount_paid.py \
		src/normalize_data.py
	in2csv $< | \
	python src/add_amount_paid.py | \
	python src/normalize_data.py "New Jersey" \
		--date_opened "Case Entry Date" \
		--employer_name "Employer Trade Name" \
		--amount_assessed "Final Wages" \
		--amount_paid amount_paid \
	> $@

cleanup:
	rm -f output/*