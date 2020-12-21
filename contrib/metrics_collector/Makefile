## ----------------------------------------------------------------------
## Metrics Collector Makefile
## ----------------------------------------------------------------------


MODULE_big = metrics_collector
OBJS = query_info.o metrics_collector.o socket_utils.o activity.o lock.o utils.o $(WIN32RES)
PGFILEDESC = "metrics_collector - collect query running info"
PG_CPPFLAGS = -I$(libpq_srcdir) -I$(srcdir)/include
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/metrics_collector
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif