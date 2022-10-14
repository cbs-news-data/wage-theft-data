GENERATED_FILES: output/maine.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

output/maine.csv: \
		src/normalize_data.py \
		input/CBSExtract.csv
	python $^ Maine \
		--date_opened INDATE \
		--employer_name EMPLOYERNAME \
		--amount_assessed WDWAGESDUE \
		--amount_paid WRWAGESPAID \
		> $@

cleanup:
	rm -f $(GENERATED_FILES)