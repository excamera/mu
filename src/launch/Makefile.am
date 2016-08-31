AM_CPPFLAGS = $(CXX14_FLAGS) -I$(srcdir)/../util -I$(srcdir)/../net -I$(srcdir)/../http -I$(srcdir)/../lambdareq
AM_CXXFLAGS = $(PICKY_CXXFLAGS)

lib_LTLIBRARIES = liblaunch.la
liblaunch_la_SOURCES = launch.hh launch.cc
liblaunch_la_LIBADD = ../lambdareq/liblambdareq.la ../http/libhttp.la ../net/libnet.la ../util/libutil.la $(OpenSSL_LIBS)
