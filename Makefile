SHELL := /bin/bash

DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
export SHARED_DIR := $(DIR)/shared

TASKS := $(sort $(wildcard tasks/*))
NOTEBOOKS := $(wildcard notebooks/*)

.PHONY: \
	all \
	$(TASKS) \
	$(NOTEBOOKS) \
	init \
	cleanup-all

all: $(TASKS) $(NOTEBOOKS)

$(TASKS):
	$(MAKE) -C $@

$(NOTEBOOKS):
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