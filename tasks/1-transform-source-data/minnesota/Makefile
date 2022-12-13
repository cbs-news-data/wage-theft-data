GENERATED_FILES: output/minnesota.csv

.PHONY: all cleanup

.INTERMEDIATE: output/stacked.csv

all: $(GENERATED_FILES)

include ../../../Makefile

output/minnesota.csv: \
		$(SHARED_DIR)/src/normalize_data.py \
		output/stacked.csv
	python $^ Minnesota \
		--date_opened date_received \
		--employer_name account_name \
		--employer_dba_name account_dba \
		--amount_assessed amounts_claimed_due_or_assessed \
		--amount_paid payments \
	> $@

output/stacked.csv: input/CBSDataResponse.xlsx
	echo case_number,date_received,account_name,account_dba,naics_code,status,amounts_claimed_due_or_assessed,payments > $@

	in2csv $< --sheet "Dataset1 - Legacy Database" | \
		csvcut -c "case number,date received,Account name,Account DBA,NAICS code,Status,Amounts claimed due or assessed,Payments" | \
		awk "NR > 1" \
 	>> $@

	in2csv $< --sheet "Dataset2 - Current Database" | \
		csvcut -c "Case Number,Assignment Date,Legal Name,DBA Name,NAICS Code,Status,Amount Of Claim,Amount Paid" | \
		awk "NR > 1" \
	>> $@

cleanup:
	rm -f output/*