GENERATED_FILES: output/kentucky.csv

.PHONY: all cleanup

.INTERMEDIATE: output/extracted.csv

all: $(GENERATED_FILES)

output/kentucky.csv: \
		output/extracted.csv \
		src/normalize_data.py \
		hand/converters_violation_category.yaml
	python src/normalize_data.py $< Kentucky \
		--employer_name Employer \
		--employer_city city \
		--date_closed DateClosed \
		--amount_claimed amount \
		--violation_category Violation \
		> $@

output/extracted.csv: \
		src/extract_tables.py \
		input/CBS_News_Open_Records_Request.pdf
	python $^ > $@

cleanup:
	rm -f output/*