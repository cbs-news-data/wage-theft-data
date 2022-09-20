GENERATED_FILES: output/wyoming.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

output/wyoming.csv: \
		input/wyoming.xlsx \
		src/normalize_data.py
	in2csv --skip-lines 1 $< | \
	python src/normalize_data.py Wyoming \
		--date_opened "OPENED DATE" \
		--date_closed "CLOSED DATE" \
		--amount_paid "AMOUNT RECOVERED" \
		> $@

cleanup:
	rm -f output/*