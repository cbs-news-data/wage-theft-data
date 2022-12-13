STATES := $(filter-out Makefile colorado west_virginia, $(shell ls ../1-transform-source-data))

GENERATED_FILES := $(addprefix output/, $(addsuffix -wage-theft-report.pdf, $(STATES)))

all: $(GENERATED_FILES)

$(GENERATED_FILES): \
		src/generate_reports.py \
		$(wildcard input/*)
	python $< \
		input/state_complaints.csv.gz \
		input/state_characteristics.csv \
		input/report_template.md.j2 \
		$(subst output/,,$(subst -wage-theft-report.pdf,,$@)) | \
	pandoc \
		--standalone \
		-c https://cdnjs.cloudflare.com/ajax/libs/github-markdown-css/2.10.0/github-markdown.css \
		-t html | \
	sed '/<body>/c\<body class="markdown-body">' | \
	weasyprint - $@

cleanup:
	rm -f output/*