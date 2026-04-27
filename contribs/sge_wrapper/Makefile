# Makefile for SGE wrapper plugins in Slurm contribs/sge_wrapper

PREFIX ?= $(DESTDIR)/usr
BINDIR := $(PREFIX)/bin

# Compiler and flags
CC = gcc
CFLAGS = -g -std=gnu99 -I$(PREFIX)/include -Wall -Wextra -O2
LDFLAGS = -L$(PREFIX)/lib -L$(PREFIX)/lib/slurm \
          -lslurm -lslurmfull -lm -lpthread \
          -Wl,-rpath=$(PREFIX)/lib -Wl,-rpath=$(PREFIX)/lib/slurm

# Source files and corresponding binaries
SOURCES = qsub.c qconf.c qstat.c qacct.c qmod.c qhold.c qdel.c
BINS = qsub qconf qstat qacct qmod qhold qdel

# Special cases: qrls shares qhold.c
EXTRA_BINS = qrls

# Object files (not used directly, but for clarity)
OBJECTS = $(SOURCES:.c=.o)

# Default target
all: $(BINS) $(EXTRA_BINS)

# Rule to build each binary from its source
qsub: qsub.c
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)

qconf: qconf.c
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)

qstat: qstat.c
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)

qacct: qacct.c
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)

qmod: qmod.c
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)

qhold: qhold.c
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)

qdel: qdel.c
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)

# Special rule for qrls (reuses qhold.c)
qrls: qhold.c
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)

# Install target: copy binaries to PREFIX/bin
install: all
	mkdir -p $(PREFIX)/bin
	for bin in $(BINS) $(EXTRA_BINS); do \
		cp $$bin $(PREFIX)/bin/$$bin; \
	done

# Clean target: remove binaries
clean:
	rm -f $(BINS) $(EXTRA_BINS) *.o

