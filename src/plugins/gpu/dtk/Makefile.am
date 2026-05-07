# Makefile for gpu/dtk plugin

AUTOMAKE_OPTIONS = foreign

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

AM_CPPFLAGS = -DSLURM_PLUGIN_DEBUG -I$(top_srcdir) -I$(top_srcdir)/src/common $(DTK_CPPFLAGS)

DTK_SOURCES = gpu_dtk.c

pkglib_LTLIBRARIES = gpu_dtk.la
gpu_dtk_la_SOURCES = $(DTK_SOURCES)
gpu_dtk_la_LDFLAGS = $(NUMA_LIBS) $(PLUGIN_FLAGS)
gpu_dtk_la_LIBADD = ../common/libgpu_common.la
