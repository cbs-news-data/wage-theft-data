GENERATED_FILES: output/new_hampshire.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

include ../../../Makefile

output/new_hampshire.csv: \
		input/REJhQ7wELFp096-TcN2c9OTF40WXFfFBKBu5o_j12KBthR4HvG71pjJ-V6oRwAI2REmoOge9ITCZ0FUqCrNTOHO1uYMA3GkqFUHREybtx512UNBnU7X9l4hpr95OCMoKpuprsrjtM3IczwjOChrKNS3pK7XCQ12j5Idp0xkuC7A.xlsx \
		src/fix_dates.py \
		$(PROCESSOR_DIR)/normalize_data.py \
		$(wildcard hand/*)
	in2csv --no-inference $< | python src/fix_dates.py | \
	python $(PROCESSOR_DIR)/normalize_data.py "New Hampshire" \
		--employer_name "Claim Against Employer" \
		--date_opened received_date \
		--amount_claimed "Amount Requested" \
		--amount_paid "Amount Collected" \
		--case_status "Claim Status" \
		> $@

cleanup:
	rm -f output/*