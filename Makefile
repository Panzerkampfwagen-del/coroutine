CC      ?= cc
CFLAGS  := -std=c17 -O2 -g -Wall -Wextra -Wshadow -Iinclude
ASFLAGS :=

BUILD   := build

SRC     := src/coro.c src/channel.c src/mutex.c src/switch.S
OBJ     := $(patsubst %.c,$(BUILD)/%.o,$(filter %.c,$(SRC))) \
           $(BUILD)/switch.o

.PHONY: all test sanitize ucontext-test run-echo clean

all: $(BUILD)/test_all $(BUILD)/echo_server

$(BUILD):
	mkdir -p $(BUILD)/src $(BUILD)/tests

$(BUILD)/%.o: %.c | $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD)/switch.o: src/switch.S | $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD)/test_all: tests/test_all.c $(OBJ)
	$(CC) $(CFLAGS) $^ -o $@

$(BUILD)/echo_server: examples/echo_server.c $(OBJ)
	$(CC) $(CFLAGS) $^ -o $@

# run the full suite (default backend: hand-written asm context switch)
test: all
	./$(BUILD)/test_all

# AddressSanitizer + UBSan build of the same suite
sanitize:
	$(CC) $(CFLAGS) -fsanitize=address,undefined tests/test_all.c $(SRC) -o $(BUILD)/test_asan
	ASAN_OPTIONS=detect_stack_use_after_return=0 ./$(BUILD)/test_asan

# same suite against the portable ucontext backend instead of asm
ucontext-test:
	$(CC) $(CFLAGS) -DCORO_USE_UCONTEXT tests/test_all.c \
	    $(filter-out src/switch.o,$(SRC)) -o $(BUILD)/test_ucx
	./$(BUILD)/test_ucx

run-echo: $(BUILD)/echo_server
	./$(BUILD)/echo_server 8080

clean:
	rm -rf $(BUILD)
