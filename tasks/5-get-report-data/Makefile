GENERATED_FILES: \
	output/state_characteristics.csv \
	output/state_complaints_deduped_for_report.csv.gz

.PHONY: all cleanup

all: $(GENERATED_FILES)

include ../../Makefile

output/state_complaints_deduped_for_report.csv.gz: \
		input/state_complaints.csv.gz \
		$(PROCESSOR_DIR)/get_case_duration.py
	gunzip -c $< | \
	python $(word 2, $^) | \
	gzip > $@

output/state_characteristics.csv: $(filter-out input/state_complaints.csv.gz, $(wildcard input/*))
	csvjoin --left --columns state_name $^ > $@

cleanup:
	rm -f output/*