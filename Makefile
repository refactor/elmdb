PROJECT = elmdb
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.1.0

TEST_DEPS = proper

CFLAGS += -DNODEBUG
CFLAGS += -pthread
#CFLAGS += -DMDB_DEBUG

CONFIG ?= config/sys.config
SHELL=/bin/bash
SHELL_OPTS = -config ${CONFIG}

EUNIT_ERL_OPTS = -config ${CONFIG}
PROPER_ERL_OPTS = -config ${CONFIG}
include erlang.mk
