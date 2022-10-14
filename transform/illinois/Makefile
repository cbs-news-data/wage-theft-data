GENERATED_FILES: output/illinois.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

output/illinois.csv: \
		input/FOIA-MWOT_final.xlsx \
		src/normalize_data.py
	in2csv $< | \
	python src/normalize_data.py Illinois \
		--date_opened "Filing Date" \
		--employer_name "Employer" \
		--amount_claimed TotalAmountClaimed \
	> $@

cleanup:
	rm -f $(GENERATED_FILES)