# makefile for building the libgulp extension
# this builds the same library that would be created by python's setup.py

# compiler settings
CC := gcc
CXX := g++
AR := ar

# output library name (matches what setup.py would produce)
PYTHON := python
EXT_SUFFIX := $(shell $(PYTHON) -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))")
OUTPUT_NAME := libgulp$(EXT_SUFFIX)

# find all source files
C_SOURCES := $(wildcard *.c)
CPP_SOURCES := $(wildcard *.cpp)
OBJECTS := $(C_SOURCES:.c=.o) $(CPP_SOURCES:.cpp=.o)

# flags for compiling
INCLUDEPY := $(shell $(PYTHON) -c "import sysconfig; print(sysconfig.get_config_var('INCLUDEPY'))")
LIBDIR := $(shell $(PYTHON) -c "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))")
LDVERSION := $(shell $(PYTHON) -c "import sysconfig; print(sysconfig.get_config_var('LDVERSION'))")

CFLAGS := -Wall -fPIC -O3 -I$(INCLUDEPY)
CXXFLAGS := $(CFLAGS) -std=c++11
LDFLAGS := -shared -L$(LIBDIR) -lpython$(LDVERSION)

# default target
.PHONY: all
all: $(OUTPUT_NAME)

# rule to build the shared library
$(OUTPUT_NAME): $(OBJECTS)
	@echo "linking $@"
	@$(CXX) $(LDFLAGS) -o $@ $^

# rule to compile c files
%.o: %.c
	@echo "compiling $<"
	@$(CC) $(CFLAGS) -c $< -o $@

# rule to compile c++ files
%.o: %.cpp
	@echo "compiling $<"
	@$(CXX) $(CXXFLAGS) -c $< -o $@

# clean target
.PHONY: clean
clean:
	@echo "cleaning build files"
	@rm -f $(OBJECTS) $(OUTPUT_NAME)

# install target (optional)
.PHONY: install
install: $(OUTPUT_NAME)
	@echo "installing to python package directory"
	@cp $< ../

# show help
.PHONY: help
help:
	@echo "available targets:"
	@echo "  all	 - build the shared library (default)"
	@echo "  clean   - remove compiled files"
	@echo "  install - copy the library to the parent directory"
	@echo "  help	- show this help message"