SHELL := /bin/bash

DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
export PROCESSOR_DIR := $(DIR)/processors

TASKS := $(sort $(wildcard tasks/*))

.PHONY: all $(TASKS) init cleanup-all

all: $(TASKS)

$(TASKS):
	$(MAKE) -C $@

init: \
	venv/bin/activate \
	os-dependencies.log

os-dependencies.log: apt.txt
	sudo apt-get install -y $$(cat $<) > $@

venv/bin/activate: requirements.txt
	if [ ! -f $@ ]; then virtualenv venv; fi
	source $@ && pip install -r $<
	touch $@

cleanup-all:
	find tasks -type f -path "*\output/*" -delete