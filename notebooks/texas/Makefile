GENERATED_FILES: output/texas.pdf

.PHONY: all cleanup

all: $(GENERATED_FILES)

output/texas.pdf: \
		texas.ipynb \
		$(wildcard input/*)
	jupyter nbconvert --no-input --to pdfviahtml $< --output-dir output

cleanup:
	rm -f output/*