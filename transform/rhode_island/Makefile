SHELL := /bin/bash

ENERATED_FILES: output/rhode_island.csv

.PHONY: all cleanup

.INTERMEDIATE: output/stacked.csv

all: $(GENERATED_FILES)

output/rhode_island.csv: \
		output/stacked.csv \
		src/normalize_data.py \
		$(wildcard hand/*)
	cat $< | \
	python src/normalize_data.py "Rhode Island" \
		--date_opened "DATE RECEIVED" \
		--employer_name "ACCOUNT NAME" \
		--employer_dba_name "ACCOUNT DBA NAME" \
		--amount_claimed "CLAIM AMOUNT" \
		--amount_assessed "AMOUNT ASSESSED" \
		--amount_paid "PAYMENTS RECEIVED" \
		--case_status "CASE STATUS" \
		> $@

output/stacked.csv: \
		input/Copy_of_apra_pw_excel.xls \
		input/Copy_of_complaints_excel_xls.xlsx
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