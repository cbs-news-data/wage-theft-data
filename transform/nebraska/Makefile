GENERATED_FILES: output/nebraska.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

output/nebraska.csv: \
		input/WPCA_Citations.xlsx \
		src/normalize_data.py
	in2csv $< | python src/normalize_data.py Nebraska \
		--employer_name "Business Name" \
		--date_closed "Citation Date" \
		--amount_assessed "Citation Amount" \
		--paid-in-full Paid \
		> $@

cleanup:
	rm -f output/*