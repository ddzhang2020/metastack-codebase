PREFIX ?= $(DESTDIR)/opt/gridview/slurm-24.5.8
BINDIR := $(PREFIX)/bin
HDRS   := sjinfo.h aes.h xstring2.h  list2.h influx_src.h time_format.h sql_api.h json_api.h print_json.h
# SHOPTS := -shared -Wl,--version-script=version.map
CFLAGS := -Wall -Wextra -O0 -g -std=gnu99
# LDFLAGS := -lcurl -ljansson -L$(PREFIX)/lib -L$(PREFIX)/lib/slurm \
#           -lslurm -lslurmfull -lm -lpthread \
#           -Wl,-rpath=$(PREFIX)/lib -Wl,-rpath=$(PREFIX)/lib/slurm
LDFLAGS := -lcurl -ljansson 
CC := gcc

# Source file list
SRCS := sjinfo.c sjaes.c spost.c 

# Source file list generates target file list
OBJS := $(SRCS:.c=.o)

# default target
all: sjinfo sjaes spost

# # Rules for generating executable files
# sjinfo: $(OBJS)
# 	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

# sjaes: $(OBJS)
# 	$(CC) $(CFLAGS) -o $@ $^ 
# Link object files to generate executable files
sjinfo: sjinfo.o aes.o xstring2.o list2.o influx_src.o time_format.o sql_api.o json_api.o print_json.o
	$(CC) $(CFLAGS) -o $@ $^ -lcurl -ljansson

sjaes: sjaes.o aes.o xstring2.o list2.o time_format.o 
	$(CC) $(CFLAGS) -o $@ $^  -lcurl -ljansson

spost: spost.o aes.o xstring2.o list2.o influx_src.o time_format.o sql_api.o json_api.o print_json.o
	$(CC) $(CFLAGS) -o $@ $^  -lcurl -ljansson
# Installation rules
install: sjinfo sjaes spost
	install -m0755 sjinfo $(BINDIR)/
	install -m0755 sjaes $(BINDIR)/
	install -m0755 spost $(BINDIR)/
# Cleanup rules
clean:
	rm -f sjinfo sjaes spost aes.o xstring2.o list2.o influx_src.o time_format.o sql_api.o json_api.o $(OBJS)
