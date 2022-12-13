GENERATED_FILES: output/kentucky.csv

.PHONY: all cleanup

.INTERMEDIATE: output/extracted.csv

all: $(GENERATED_FILES)

include ../../../Makefile

output/kentucky.csv: \
		output/extracted.csv \
		$(SHARED_DIR)/src/normalize_data.py \
		hand/converters_violation_category.yaml
	python $(SHARED_DIR)/src/normalize_data.py $< Kentucky \
		--employer_name Employer \
		--employer_city city \
		--date_closed DateClosed \
		--amount_paid amount \
		--violation_category Violation \
		> $@

output/extracted.csv: \
		src/extract_tables.py \
		input/CBS_News_Open_Records_Request.pdf
	python $^ > $@

cleanup:
	rm -f output/*