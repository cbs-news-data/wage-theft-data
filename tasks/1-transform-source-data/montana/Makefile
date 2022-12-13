GENERATED_FILES: output/montana.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

include ../../../Makefile

output/montana.csv: \
		input/Wage_claim_stats.xlsx \
		$(SHARED_DIR)/src/fix_case_status.py \
		$(SHARED_DIR)/src/normalize_data.py \
		$(wildcard hand/*)
	in2csv $< | \
	python $(SHARED_DIR)/src/fix_case_status.py ICCU_CASE_STS_TYP_DESC "PAYMENTS RCVD" | \
	python $(SHARED_DIR)/src/normalize_data.py Montana \
		--date_opened CASE_RECEIVED \
		--employer_name RESPONDENT_NAME \
		--amount_claimed PETITIONER_CLAIMED \
		--amount_assessed PETITIONER_AMT_DUE \
		--amount_paid "PAYMENTS RCVD" \
		--case_status ICCU_CASE_STS_TYP_DESC \
	> $@

cleanup:
	rm -f output/*