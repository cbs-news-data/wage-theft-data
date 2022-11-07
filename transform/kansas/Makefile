SHELL := /bin/bash

GENERATED_FILES: output/kansas.csv

.PHONY: all cleanup

.INTERMEDIATE: output/stacked.csv

all: $(GENERATED_FILES)

output/kansas.csv: \
		src/normalize_data.py \
		output/stacked.csv \
		$(wildcard hand/*)
	python $< output/stacked.csv Kansas \
		--employer_name Employer \
		--date_opened Received \
		--date_closed Closed \
		--amount_claimed Wages \
		--amount_paid Amount \
		--case_status Notes \
		> $@

output/stacked.csv: input/CBS_Request.xlsx
	for sheet in $$(in2csv -n $<) ; do \
		if [[ $$sheet == "2010" ]] ; then \
			in2csv --sheet $$sheet $< | csvcut -c "1-12" > $@ ; \
		else \
			in2csv --sheet $$sheet $< | awk "NR > 0" | csvcut -c "1-12" >> $@ ; \
		fi ; \
	done

cleanup:
	rm -f output/*