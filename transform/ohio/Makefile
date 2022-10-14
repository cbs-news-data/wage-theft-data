GENERATED_FILES: output/ohio.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

output/ohio.csv: \
		input/UPDATED_WH_Complaints.xlsx \
		src/normalize_data.py
	in2csv $< | \
	csvgrep -c "Date Received" -r "^.+$$" | \
	python src/normalize_data.py Ohio \
		--date_opened "Date Received" \
		--employer_name "Account Name" \
		--amount_claimed "Claim Amount" \
		--amount_assessed "Amount Assessed" \
		--amount_paid "Payment Received" \
	> $@

cleanup:
	rm -f output/*