CC      ?= cc
CFLAGS  := -std=c17 -O2 -g -Wall -Wextra -Wshadow -Iinclude
ASFLAGS :=

BUILD   := build

SRC     := src/coro.c src/channel.c src/mutex.c src/switch.S
LDLIBS  :=

# io_uring backend is compiled in when liburing is present; the suite's I/O
# tests skip themselves gracefully where it is missing (CI sandboxes).
ifeq ($(shell test -f /usr/include/liburing.h && echo yes),yes)
    SRC += src/io.c
    LDLIBS += -luring
    CFLAGS += -DCORO_HAVE_IO_URING
endif

OBJ     := $(patsubst %.c,$(BUILD)/%.o,$(filter %.c,$(SRC))) \
           $(BUILD)/switch.o

.PHONY: all test sanitize ucontext-test fuzz bench run-echo clean

all: $(BUILD)/test_all $(BUILD)/echo_server $(BUILD)/microdb

$(BUILD):
	mkdir -p $(BUILD)/src $(BUILD)/tests

$(BUILD)/%.o: %.c | $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD)/switch.o: src/switch.S | $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD)/test_all: tests/test_all.c $(OBJ)
	$(CC) $(CFLAGS) $^ -o $@ $(LDLIBS)

$(BUILD)/echo_server: examples/echo_server.c $(OBJ)
	$(CC) $(CFLAGS) $^ -o $@ $(LDLIBS)

# run the full suite (default backend: hand-written asm context switch)
test: all
	./$(BUILD)/test_all

# AddressSanitizer + UBSan build of the same suite
sanitize:
	$(CC) $(CFLAGS) -fsanitize=address,undefined tests/test_all.c $(SRC) -o $(BUILD)/test_asan $(LDLIBS)
	ASAN_OPTIONS=detect_stack_use_after_return=0 ./$(BUILD)/test_asan

# same suite against the portable ucontext backend instead of asm
ucontext-test:
	$(CC) $(CFLAGS) -DCORO_USE_UCONTEXT tests/test_all.c $(SRC) -o $(BUILD)/test_ucx $(LDLIBS)
	./$(BUILD)/test_ucx

$(BUILD)/microdb: apps/microdb/microdb.c apps/microdb/resp.h $(OBJ)
	$(CC) $(CFLAGS) -Iapps/microdb $< -o $@ $(filter-out build/src/io.o,$(OBJ)) $(BUILD)/src/io.o $(LDLIBS)

# both fuzzers: randomized producer/consumer workloads checked against
# invariants the scheduler must uphold (lost items, lost wake-ups, mutex
# lost updates; FIFO order vs a reference model)
$(BUILD)/fuzz_sched: tests/fuzz/sched_fuzz.c $(OBJ)
	$(CC) $(CFLAGS) $^ -o $@ $(LDLIBS)

$(BUILD)/fuzz_chan: tests/fuzz/channel_fuzz.c $(OBJ)
	$(CC) $(CFLAGS) $^ -o $@ $(LDLIBS)

fuzz: $(BUILD)/fuzz_sched $(BUILD)/fuzz_chan
	./$(BUILD)/fuzz_sched 20000
	./$(BUILD)/fuzz_chan 20000

# primitive micro-benchmarks: context switch, channel rendezvous, scaling
$(BUILD)/bench_prim: benchmarks/bench_prim.c $(OBJ)
	$(CC) $(CFLAGS) $^ -o $@ $(LDLIBS)

bench: $(BUILD)/bench_prim
	./$(BUILD)/bench_prim

# TCP echo load test (io_uring vs nonblock backends):
#   make run-bench                     -> default backend, 1000 conns
#   ./build/echo_server --no-uring &   -> then bench.py against :8080
run-bench: $(BUILD)/echo_server
	python3 benchmarks/bench.py --spawn build/echo_server --conns 1000 \
	    --duration 5 --out build/bench_uring.json

run-microdb: $(BUILD)/microdb
	./$(BUILD)/microdb

run-echo: $(BUILD)/echo_server
	./$(BUILD)/echo_server

clean:
	rm -rf $(BUILD)
