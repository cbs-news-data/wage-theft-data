GENERATED_FILES: output/michigan.csv

.PHONY: all cleanup

all: $(GENERATED_FILES)

include ../../../Makefile

output/michigan.csv: \
		input/Copy_of_WHD-FOIA_Request_22-08-015_REPORT).xlsx \
		$(SHARED_DIR)/src/normalize_data.py \
		hand/converters_case_status.yaml
	in2csv "$<" | \
	python $(SHARED_DIR)/src/normalize_data.py Michigan \
		--date_opened RECEIPT_DATE \
		--employer_name EMPLOYER_NAME \
		--case_status OPEN_OR_CLOSED \
		--amount_claimed CLAIM_AMOUNT \
		--amount_assessed AMOUNT_ASSESSED \
		--amount_paid PAYMENT_RECIEVED \
	> $@

cleanup:
	rm -f output/*