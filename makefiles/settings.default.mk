COMPILER ?= autog++
# options: debug|release
BLD ?= debug

# WITH_COVERAGE := 1
# WITH_PAPI :=
# WITH_V8 := 0
# WITH_PROFILER := 1

# Per Default HYRISE is compiled with MySQL support, set to 0 to disable
# WITH_MYSQL := 1

# persistency options: NONE | SIMPLELOGGER | BUFFEREDLOGGER | NVRAM
PERSISTENCY := NONE
VERBOSE_BUILD := 1

# for NVRAM persistency, a set of further options can be specified
# [NVRAM_FILESIZE is specified in MB]
#NVRAM_MOUNTPOINT := /mnt/pmfs
#NVRAM_FILENAME := hyrise
#NVRAM_FILESIZE := 1024
#NVSIMULATOR_FLUSH_NS := 0
#NVSIMULATOR_READ_NS := 0
#NVSIMULATOR_WRITE_NS := 0

include mysql.mk
