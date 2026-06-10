CC      ?= gcc
WARN    := -Wall -Wextra
STD     := -std=c17
INC     := -Iinclude

# Build mode knobs. The default is optimized with debug info; `make debug` and
# `make release` override OPT/SAN/MODE via recursive make (see below).
OPT     ?= -O2 -g
SAN     ?=
MODE    ?= default
CFLAGS   = $(OPT) $(SAN) $(WARN) $(STD) $(INC)
LDFLAGS  = $(SAN)
LDLIBS   =

# Library sources are discovered automatically.
SRC_C   := $(wildcard src/*.c)
SRC_S   := $(wildcard src/*.S)
OBJ     := $(SRC_C:.c=.o) $(SRC_S:.S=.o)

LIB     := libcoro.a
EX_SRC  := $(wildcard examples/*.c)
EX_BIN  := $(notdir $(EX_SRC:.c=))

# Files that clang-format owns (everything but the hand-written assembly).
FMT_FILES := $(wildcard include/*.h src/*.c src/*.h examples/*.c \
                        examples/echo_server/*.c apps/microdb/*.c \
                        apps/microdb/*.h tests/stress/*.c tests/fuzz/*.c)

.PHONY: all test check tests clean debug release format format-check tidy \
        cppcheck aarch64-check dist-check FORCE

all: $(LIB) $(EX_BIN) echo_server microdb microdb_proxy

# --- build-mode tracking ----------------------------------------------------
# Object files compiled with different flags (default/debug/release) are
# indistinguishable to Make by timestamp alone, so switching modes would
# otherwise reuse stale objects or link mismatched libraries. The objects depend
# on a stamp file holding the active mode; the stamp is rewritten only when the
# mode actually changes, which forces exactly the rebuilds that are needed.
.build-mode: FORCE
	@[ -f $@ ] && [ "`cat $@`" = "$(MODE)" ] || { echo "$(MODE)" > $@; }

FORCE:

# --- library + examples -----------------------------------------------------

$(LIB): $(OBJ)
	ar rcs $@ $^

src/%.o: src/%.c .build-mode
	$(CC) $(CFLAGS) -c $< -o $@

src/%.o: src/%.S .build-mode
	$(CC) $(CFLAGS) -c $< -o $@

# The simple echo example uses the io_uring layer, so it needs liburing too.
echo: LDLIBS += -luring

$(EX_BIN): %: examples/%.c $(LIB)
	$(CC) $(CFLAGS) $(LDFLAGS) $< -o $@ -L. -lcoro $(LDLIBS)

# Standalone demo application (its own subdirectory). Needs liburing.
echo_server: examples/echo_server/echo_server.c $(LIB)
	$(CC) $(CFLAGS) $(LDFLAGS) $< -o $@ -L. -lcoro -luring

# microdb: a Redis-compatible in-memory store built on the runtime.
microdb: apps/microdb/microdb.c apps/microdb/resp.h $(LIB)
	$(CC) $(CFLAGS) $(LDFLAGS) $< -o $@ -L. -lcoro -luring

# microdb-proxy: sharding + read-routing + failover front-end for microdb.
microdb_proxy: apps/microdb/microdb_proxy.c apps/microdb/resp.h $(LIB)
	$(CC) $(CFLAGS) $(LDFLAGS) $< -o $@ -L. -lcoro -luring

# --- tests ------------------------------------------------------------------

stress: tests/stress/stress.c $(LIB)
	$(CC) $(CFLAGS) $(LDFLAGS) $< -o $@ -L. -lcoro $(LDLIBS)

channel_fuzz: tests/fuzz/channel_fuzz.c $(LIB)
	$(CC) $(CFLAGS) $(LDFLAGS) $< -o $@ -L. -lcoro $(LDLIBS)

sched_fuzz: tests/fuzz/sched_fuzz.c $(LIB)
	$(CC) $(CFLAGS) $(LDFLAGS) $< -o $@ -L. -lcoro $(LDLIBS)

tests: stress channel_fuzz sched_fuzz

# Cross-compile the runtime for aarch64 and run it under qemu-user (verifies the
# AArch64 context switch). Requires gcc-aarch64-linux-gnu and qemu-user-static.
aarch64-check:
	tests/arch/run_aarch64.sh

# End-to-end test of the distributed layer: spawns microdb primaries/replicas
# and the proxy, then checks replication, sharding and failover. Needs python3
# (speaks RESP over raw sockets - no redis-cli required).
dist-check: microdb microdb_proxy
	python3 tests/dist/dist_test.py

# `make test` runs the two canonical examples (as in the original spec).
test: hello pingpong
	@echo "== hello =="; ./hello
	@echo "== pingpong =="; ./pingpong

# `make check` builds and runs the full suite.
check: all tests
	@echo "== hello =="; ./hello >/dev/null && echo "ok"
	@echo "== pingpong =="; ./pingpong
	@echo "== stress =="; ./stress 5
	@echo "== channel_fuzz =="; ./channel_fuzz 20000
	@echo "== sched_fuzz =="; ./sched_fuzz 20000

# --- build modes ------------------------------------------------------------

debug:
	$(MAKE) all tests MODE=debug \
	    OPT='-O0 -g3 -fno-omit-frame-pointer' \
	    SAN='-fsanitize=address,undefined'

release:
	$(MAKE) all MODE=release OPT='-O2 -march=native -DNDEBUG' SAN=

# --- code quality -----------------------------------------------------------

format:
	clang-format -i $(FMT_FILES)

format-check:
	clang-format --dry-run -Werror $(FMT_FILES)

tidy:
	clang-tidy $(SRC_C) examples/echo_server/echo_server.c \
	    apps/microdb/microdb.c apps/microdb/microdb_proxy.c -- $(STD) $(INC)

cppcheck:
	cppcheck --enable=warning,performance,portability --std=c17 \
	    --error-exitcode=1 --inline-suppr \
	    --suppressions-list=.cppcheck-suppressions \
	    $(INC) src examples apps tests

clean:
	rm -f $(OBJ) $(LIB) $(EX_BIN) echo_server microdb microdb_proxy stress \
	    channel_fuzz sched_fuzz .build-mode
