CC = gcc
CFLAGS = -O3 -Wall -Wextra -std=c99 -Iinclude
LDFLAGS = -lpthread

SRC_DIR = src
INC_DIR = include
BUILD_DIR = build
TEST_DIR = tests
BENCH_DIR = bench

TARGET = $(BUILD_DIR)/libraft_core.a
TEST_TARGET = $(BUILD_DIR)/run_tests
BENCH_TARGET = $(BUILD_DIR)/run_bench

SRC = $(wildcard $(SRC_DIR)/*.c)
OBJ = $(patsubst $(SRC_DIR)/%.c, $(BUILD_DIR)/%.o, $(SRC))

TEST_SRC = $(wildcard $(TEST_DIR)/*.c)
BENCH_SRC = $(wildcard $(BENCH_DIR)/*.c)

.PHONY: all build test clean benchmark directories

all: directories build test benchmark

directories:
	@mkdir -p $(BUILD_DIR)

build: directories $(TARGET)

$(TARGET): $(OBJ)
	ar rcs $@ $^

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	$(CC) $(CFLAGS) -c $< -o $@

test: directories $(TEST_TARGET)
	@if [ -f $(TEST_TARGET) ]; then ./$(TEST_TARGET); else echo "No tests found."; fi

$(TEST_TARGET): $(OBJ) $(TEST_SRC)
	@if [ -n "$(TEST_SRC)" ]; then \
		$(CC) $(CFLAGS) $(TEST_SRC) $(OBJ) -o $@ $(LDFLAGS); \
	fi

benchmark: directories $(BENCH_TARGET)
	@if [ -f $(BENCH_TARGET) ]; then ./$(BENCH_TARGET); else echo "No benchmarks found."; fi

$(BENCH_TARGET): $(OBJ) $(BENCH_SRC)
	@if [ -n "$(BENCH_SRC)" ]; then \
		$(CC) $(CFLAGS) $(BENCH_SRC) $(OBJ) -o $@ $(LDFLAGS); \
	fi

clean:
	@rm -rf $(BUILD_DIR)