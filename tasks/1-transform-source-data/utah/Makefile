GENERATED_FILES: output/utah.csv

.PHONY: all cleanup

.INTERMEDIATE: \
		output/amounts.csv \
		output/date_opened.csv \
		output/employer_name.csv

all: $(GENERATED_FILES)

include ../../../Makefile

output/utah.csv: \
		output/amounts.csv \
		output/date_opened.csv \
		output/employer_name.csv \
		src/merge_files.py \
		$(SHARED_DIR)/src/fix_case_status.py \
		$(SHARED_DIR)/src/normalize_data.py \
		$(wildcard hand/*)
	python src/merge_files.py $^ | \
	python $(SHARED_DIR)/src/fix_case_status.py status amount_received | \
	python $(SHARED_DIR)/src/normalize_data.py Utah \
		--employer_name employer_name \
		--date_opened date_opened \
		--amount_claimed amount_claimed \
		--amount_assessed amount_assessed \
		--amount_paid amount_received \
		--case_status status \
	> $@

output/amounts.csv: \
		bin/tabula-1.0.5-jar-with-dependencies.jar \
		input/DataWindow_Case_Number_Amounts_Status.pdf
	echo case_number,amount_claimed,amount_assessed,amount_received,status > $@
	java -jar $^ --pages all | \
		grep -Pve "Total|Amount" \
	>> $@

output/date_opened.csv: \
		bin/tabula-1.0.5-jar-with-dependencies.jar \
		input/DataWindow_Date_Received.pdf
	echo case_number,date_opened,description > $@
	java -jar $^ --pages all | \
		grep -Pve "Action Description" \
	>> $@

output/employer_name.csv: \
		bin/tabula-1.0.5-jar-with-dependencies.jar \
		input/DataWindow_Employer_Name.pdf
	echo case_number,employer_name,relationship_type > $@
	java -jar $^ --pages all | \
		grep -Pve "Case Number|Relationship Type" \
	>> $@

cleanup:
	rm -f output/*