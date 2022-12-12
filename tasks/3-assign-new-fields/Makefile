GENERATED_FILES: output/state_complaints.csv.gz output/texas_amounts.csv.gz

.PHONY: all cleanup

all: $(GENERATED_FILES)

output/state_complaints.csv.gz: \
		input/state_complaints.csv.gz \
		src/determine_case_outcome.py \
		src/assign_overall_case_amount.py \
		src/get_case_duration.py
	gunzip -c $< | \
	python src/determine_case_outcome.py | \
	python src/assign_overall_case_amount.py | \
	python src/get_case_duration.py | \
	gzip > $@

output/texas_amounts.csv.gz: \
		input/ORR_R005317-081222_from_CBS__C._Hacker__File_date___Amts.xlsx \
		src/assign_overall_case_amount.py
	in2csv $< | \
	python src/assign_overall_case_amount.py --colnames CLAIMED,AWARD_AM,PAID | \
	gzip > $@

cleanup:
	rm -f output/*