GENERATED_FILES: output/state_complaints.csv.gz

.PHONY: all ln_transform cleanup

all: $(GENERATED_FILES)

output/state_complaints.csv.gz: $(wildcard input/*.csv)
	csvstack $^ | gzip > $@

cleanup:
	rm -f $(GENERATED_FILES)