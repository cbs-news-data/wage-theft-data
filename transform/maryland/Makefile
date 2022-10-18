GENERATED_FILES: output/maryland.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

output/maryland.csv: \
		input/MPIA_-_CBSNews_ChristopherHacker_wagetheft.xls \
		src/normalize_data.py
	in2csv $< | python src/normalize_data.py \
		--employer_name CompanyName \
		--date_opened DateFormReceived \
		--amount_claimed "Amount Claimed" \
		--amount_paid "MoniesCollected" \
		Maryland > $@

cleanup:
	rm -f output/*
