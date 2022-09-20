SHELL := /bin/bash

ENERATED_FILES: output/rhode_island.csv

.PHONY: all cleanup

.INTERMEDIATE: output/stacked.csv

all: $(GENERATED_FILES)

output/rhode_island.csv: \
		output/stacked.csv \
		src/normalize_data.py
	cat $< | \
	csvgrep -c "ACCOUNT NAME" -r "^$$" --invert-match | \
	python src/normalize_data.py "Rhode Island" \
		--date_opened "DATE RECEIVED" \
		--employer_name "ACCOUNT NAME" \
		--employer_dba_name "ACCOUNT DBA NAME" \
		--amount_claimed "CLAIM AMOUNT" \
		--amount_assessed "AMOUNT ASSESSED" \
		--amount_paid "PAYMENTS RECEIVED" \
		> $@

output/stacked.csv: \
		input/rhode_island.xls \
		input/rhode_island1.xlsx
	i=0 ; \
	for file in $^ ; do \
		if [[ $$i -eq 0 ]] ; then \
			in2csv --skip-lines 7 $$file > $@ ; \
		else \
			in2csv --skip-lines 7 $$file | awk "NR > 1" >> $@ ; \
		fi ; \
		i=$$(($$i+1)) ; \
	done

cleanup:
	rm -f output/*