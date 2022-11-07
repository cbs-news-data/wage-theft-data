GENERATED_FILES: output/illinois.csv

.PHONY: all cleanup

.INTERMEDIATE: output/stacked.csv

all: $(GENERATED_FILES)

output/illinois.csv: \
		output/stacked.csv \
		src/normalize_data.py \
		$(wildcard hand/*)
	python src/normalize_data.py $< Illinois \
		--date_opened "date" \
		--employer_name "employer" \
		--amount_claimed amount \
		--case_status status \
	> $@

output/stacked.csv: \
		input/FOIA-MWOT_final.xlsx \
		input/FOIA-WC_Final.xlsx
	echo date,employer,status,amount > $@
	for file in $^ ; do \
		in2csv $$file | awk "NR > 1" >> $@ ; \
	done

cleanup:
	rm -f output/*