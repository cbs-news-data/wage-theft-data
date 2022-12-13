GENERATED_FILES: output/west_virginia.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

include ../../../Makefile

output/west_virginia.csv: \
		input/RFA_FOIA_REQUEST_8_1_2022.xlsx \
		$(SHARED_DIR)/src/normalize_data.py \
		hand/converters_violation_category.yaml
	in2csv $< | \
	grep -v -e ",,,,,," | \
	python $(SHARED_DIR)/src/normalize_data.py "West Virginia" \
		--employer_name "Company/ Person" \
		--violation_category "Complaint Type" \
		--date_opened "Date Assigned" \
		--date_closed "Case Closed" \
		> $@

cleanup:
	rm -f output/*