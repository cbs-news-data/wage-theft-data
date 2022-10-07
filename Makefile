SHELL := /bin/bash

TASKS := \
    transform \
	ln_input_output \
	merge \
	upload

.PHONY: \
	  all \
	  init \
	  cleanup \
	  $(TASKS)

all: $(TASKS)

$(TASKS): venv/bin/activate
	source $< && $(MAKE) -C $@

ln_input_output:
	cd merge/state_complaints/input ; \
		ln -sf ../../../transform/*/output/*.csv . ; \
		cd ../..

init: venv/bin/activate
	source $<

venv/bin/activate: requirements.txt
	if [ ! -f $@ ]; then virtualenv venv; fi
	source $@ && pip install -r $<
	touch $@
 
cleanup:
	for d in $(TASKS) ; do \
		cd "$(shell pwd)/$$d" && make cleanup  ; \
	done
