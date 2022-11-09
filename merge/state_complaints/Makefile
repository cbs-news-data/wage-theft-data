GENERATED_FILES: output/state_complaints.csv.gz

.PHONY: all ln_transform cleanup

all: $(GENERATED_FILES)

output/state_complaints.csv.gz: \
		$(wildcard input/*.csv) \
		src/schema_validate.py
	csvstack $(wildcard input/*.csv) | \
	python src/schema_validate.py | \
	gzip \
	> $@

cleanup:
	rm -f output/*