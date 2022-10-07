.INTERMEDIATE: assets

.PHONY: all upload cleanup

all: upload

upload: upload.log

upload.log: \
		src/kaggle-upload.sh \
		hand/dataset-metadata.json \
		input/state_complaints.csv \
		.env
	export $$(cat .env | xargs) && ./$< > upload.log

cleanup:
	rm -f upload.log