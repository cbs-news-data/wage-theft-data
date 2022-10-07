GENERATED_FILES: output/state_complaints.csv

.PHONY: all ln_transform cleanup

all: $(GENERATED_FILES)

output/state_complaints.csv: $(wildcard input/*.csv)
	make ln_transform
	csvstack $^ | csvsort -c uuid > $@

ln_transform:
	cd input ; \
	ln -sf ../../../transform/*/output/*.csv . ; \
	cd ..

cleanup:
	rm -f output/*