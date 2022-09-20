GENERATED_FILES: output/colorado.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

output/colorado.csv: \
		input/colorado.html \
		src/read_html.py \
		src/normalize_data.py
	python src/read_html.py $<  | \
	python src/normalize_data.py Colorado \
		--employer_name Employer \
		--date_closed Date \
	> $@

cleanup:
	rm -f output/*