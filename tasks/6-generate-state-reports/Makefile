STATES := $(filter-out Makefile colorado west_virginia, $(shell ls ../1-transform-source-data))

GENERATED_FILES := $(addprefix output/, $(addsuffix -wage-theft-report.md, $(STATES)))

include ../../Makefile

all: $(GENERATED_FILES)

$(GENERATED_FILES): \
		$(PROCESSOR_DIR)/generate_reports.py \
		$(wildcard input/*)
	python $< \
		input/state_complaints_deduped_for_report.csv.gz \
		input/state_characteristics.csv \
		input/report_template.md.j2 \
		$@ \
		$(subst output/,,$(subst -wage-theft-report.md,,$@))

cleanup:
	rm -f output/*