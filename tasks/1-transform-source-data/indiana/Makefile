GENERATED_FILES: output/indiana.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

include ../../../Makefile

output/indiana.csv: \
		input/2019-2022.xlsx \
		input/Copy_of_Wage_Claim_APRA_2010-2019.xlsx \
		src/assign_amount_assessed.py \
		$(SHARED_DIR)/src/normalize_data.py \
		$(wildcard hand/*)
	in2csv $< | \
	python src/assign_amount_assessed.py | \
	python $(SHARED_DIR)/src/normalize_data.py Indiana \
		--employer_name "Employer Name" \
		--date_opened "Date Received" \
		--amount_claimed "Amount of Claim" \
		--amount_assessed amount_assessed \
		--amount_paid "Net Paid" \
		--case_status "Claim Status" \
	> $@
	
	in2csv $(word 2, $^) | \
	python src/assign_amount_assessed.py | \
	python $(SHARED_DIR)/src/normalize_data.py Indiana \
		--employer_name Employer \
		--date_opened "ClaimReceived" \
		--date_paid "Date Paid" \
		--amount_claimed "Amount Claimed" \
		--amount_assessed amount_assessed \
		--amount_paid "Net Paid" \
		--case_status Disposition | \
	awk "NR > 1" \
	>> $@

cleanup:
	rm -f output/*