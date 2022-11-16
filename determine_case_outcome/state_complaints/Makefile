GENERATED_FILES: output/state_complaints.csv.gz

.PHONY: all cleanup

all: $(GENERATED_FILES)

output/state_complaints.csv.gz: \
		src/determine_case_outcome.py \
		input/state_complaints.csv.gz
	python $^ $@

cleanup:
	rm -f output/*