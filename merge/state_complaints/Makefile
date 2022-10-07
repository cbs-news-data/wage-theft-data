GENERATED_FILES: output/state_complaints.csv

.PHONY: all ln_transform cleanup

all: $(GENERATED_FILES)

output/state_complaints.csv: $(wildcard input/*.csv)
	csvstack $^ | csvsort -c uuid > $@

cleanup:
	rm -f output/*