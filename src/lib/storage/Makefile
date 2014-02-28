hyr-storage := $(realpath $(dir $(lastword $(MAKEFILE_LIST))))

-include ../../../rules.mk

include $(PROJECT_ROOT)/src/lib/helper/Makefile
include $(PROJECT_ROOT)/third_party/Makefile

hyr-storage.libname := hyr-storage
hyr-storage.libs := hwloc rt
hyr-storage.deps := hyr-helper ftprinter cereal optional

ifeq ($(WITH_FOLLY), 1)
hyr-access.libs += folly
endif

$(eval $(call library,hyr-storage))
