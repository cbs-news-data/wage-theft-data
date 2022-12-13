GENERATED_FILES: output/nebraska.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

include ../../../Makefile

output/nebraska.csv: \
		input/WPCA_Citations.xlsx \
		src/assign_case_status.py \
		$(SHARED_DIR)/src/normalize_data.py \
		$(wildcard hand/*)
	in2csv $< | \
	python src/assign_case_status.py | \
	python $(SHARED_DIR)/src/normalize_data.py Nebraska \
		--employer_name "Business Name" \
		--date_closed "Citation Date" \
		--amount_assessed "Citation Amount" \
		--paid-in-full Paid \
		--case_status case_status \
	> $@

cleanup:
	rm -f output/*