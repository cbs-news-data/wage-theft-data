GENERATED_FILES: output/wyoming.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

include ../../../Makefile

output/wyoming.csv: \
		input/AdHoc_Report_64_CBS_request.xlsx \
		$(PROCESSOR_DIR)/normalize_data.py
	in2csv --skip-lines 1 $< | \
	python $(PROCESSOR_DIR)/normalize_data.py Wyoming \
		--date_opened "OPENED DATE" \
		--date_closed "CLOSED DATE" \
		--amount_paid "AMOUNT RECOVERED" \
		> $@

cleanup:
	rm -f output/*