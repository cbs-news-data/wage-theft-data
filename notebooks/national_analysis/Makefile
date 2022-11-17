GENERATED_FILES: output/national_analysis.pdf

.PHONY: all cleanup

all: $(GENERATED_FILES)

output/national_analysis.pdf: \
		national_analysis.ipynb \
		$(wildcard input/*) \
		$(wildcard src/*)
	jupyter nbconvert --no-input --to pdfviahtml $< --output-dir output

cleanup:
	rm -f output/*