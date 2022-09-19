GENERATED_FILES: output/montana.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

output/montana.csv: \
		input/montana.xlsx \
		src/normalize_data.py
	in2csv $< | python src/normalize_data.py Montana \
		--date_opened CASE_RECEIVED \
		--employer_name RESPONDENT_NAME \
		--amount_claimed PETITIONER_CLAIMED \
		--amount_assessed PETITIONER_AMT_DUE \
		--amount_paid "PAYMENTS RCVD" \
		> $@

cleanup:
	rm -f output/*