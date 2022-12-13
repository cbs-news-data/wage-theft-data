GENERATED_FILES: output/maryland.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

include ../../../Makefile

output/maryland.csv: \
		input/MPIA_CBS_News_Hacker.xls \
		$(SHARED_DIR)/src/normalize_data.py
	in2csv $< | python $(SHARED_DIR)/src/normalize_data.py \
		--employer_name CompanyName \
		--date_opened DateFormReceived \
		--amount_claimed "Total Amt Claimed" \
		--amount_paid "Monies Collected" \
		Maryland > $@

cleanup:
	rm -f output/*
