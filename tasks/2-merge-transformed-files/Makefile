GENERATED_FILES: output/state_complaints.csv.gz

.PHONY: all ln_transform cleanup

all: $(GENERATED_FILES)

output/state_complaints.csv.gz: \
		$(wildcard input/*.csv) \
		src/schema_validate.py \
		# watch all output files so any new files get linked
		$(wildcard ../../../tasks/1-transform-source-data/*/output/*)
	ln -sf ../../../tasks/1-transform-source-data/*/output/* input/
	csvstack $(wildcard input/*.csv) | \
	python src/schema_validate.py | \
	gzip \
	> $@

cleanup:
	rm -f output/*