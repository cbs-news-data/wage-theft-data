GENERATED_FILES: output/state_complaints.csv

.PHONY: all ln_transform cleanup

all: $(GENERATED_FILES)

output/state_complaints.csv: $(wildcard input/*.csv)
	csvstack $^ | csvsort -c case_uuid,violation_uuid | gzip > $@

cleanup:
	rm -f $(GENERATED_FILES)