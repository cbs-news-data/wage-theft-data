GENERATED_FILES: output/state_characteristics.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

include ../../Makefile

output/state_characteristics.csv: $(filter-out input/state_complaints.csv.gz, $(wildcard input/*))
	csvjoin --left --columns state_name $^ > $@

cleanup:
	rm -f output/*