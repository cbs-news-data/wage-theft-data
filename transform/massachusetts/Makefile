GENERATED_FILES: output/massachusetts.csv

.PHONY: all cleanup

.INTERMEDIATE: output/violations_exploded.csv

all: $(GENERATED_FILES)

output/massachusetts.csv: \
		output/violations_exploded.csv \
		src/normalize_data.py \
		hand/converters_violation_category.yaml
	cat $< | python src/normalize_data.py Massachusetts \
		--citation_sequence citation_sequence \
		--employer_name Employer \
		--employer_dba_name DBA \
		--employer_city city \
		--violation_category violation_description \
		--date_opened "Date Issued" \
		--amount_assessed "Total Assessed" \
		--paid_in_full "Paid In Full" \
		--appeal_filed "Appeal Filed" \
		--lien_issued "Lien Issued" \
		--specific_intent "With Specific Intent" \
		> $@

output/violations_exploded.csv: \
		src/clean.py \
		input/massachusetts.xlsx
	python $^ > $@

cleanup:
	rm -f output/*