# this makefile is used to run all task subdirectories in a given task folder
# it is symlinked to the task directory to avoid repetition

SUBDIRS := $(wildcard */.)

.PHONY: all $(SUBDIRS)

all: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@

cleanup:
	find . -type f -path "*/output/*" -delete