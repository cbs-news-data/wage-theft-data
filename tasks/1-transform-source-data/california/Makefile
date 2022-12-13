GENERATED_FILES: output/california.csv

.PHONY: all cleanup

.INTERMEDIATE: output/stacked.csv

all: $(GENERATED_FILES)

include ../../../Makefile

output/california.csv: \
		$(SHARED_DIR)/src/normalize_data.py \
		output/stacked.csv
	python $^ California \
		--employer_name account_name \
		--date_opened date_of_docket \
		--date_closed oda_decision_date \
		--amount_claimed original_amount_payable_to_ee_s \
		--amount_assessed ee_s_amt_assessed \
		--amount_paid ee_payments_rec_d \
	> $@

output/stacked.csv: \
		src/transform_ca_claims_data.py \
		$(wildcard input/*)
	python $^ > $@

cleanup:
	rm -f output/*