GENERATED_FILES: output/ohio.csv

.PHONY: all cleanup

.INTERMEDIATE: output/stacked.csv

all: $(GENERATED_FILES)

output/ohio.csv: \
		src/normalize_data.py \
		output/stacked.csv
	python $^ Ohio \
		--date_opened date_opened \
		--employer_name employer_name \
		--amount_claimed amount_claimed \
		--amount_assessed amount_assessed \
		--amount_paid amount_paid \
	> $@

output/stacked.csv: \
		input/UPDATED_WH_Complaints.xlsx \
		input/WageAndHour-Report-10-14-2022.xlsx
	echo employer_name,date_opened,violation_category,status,amount_claimed,amount_assessed,amount_paid > $@

	in2csv $< | \
		csvgrep -c "Date Received" -r "^.+$$" | \
		csvcut -c "Account Name","Date Received","Complaint Type","Case Status","Claim Amount","Amount Assessed","Payment Received" | \
		awk "NR > 1" \
	>> $@

	in2csv -K 5 input/WageAndHour-Report-10-14-2022.xlsx | \
		csvcut -C a,b | \
		csvcut -c FolderName,InDate,FolderType,StatusDesc | \
		csvgrep -c InDate -r "^.+$$" | \
		awk "NR > 1" \
	>> $@

cleanup:
	rm -f output/*