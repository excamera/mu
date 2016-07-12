AM_CPPFLAGS = $(CXX14_FLAGS) \
			  -I$(srcdir)/../util -I$(srcdir)/../net \
			  -I$(srcdir)/../http -I$(srcdir)/../lambdareq
AM_CXXFLAGS = $(PICKY_CXXFLAGS)

bin_PROGRAMS = hello launchser launchpar launchpipe

hello_SOURCES = hello.cc
hello_LDADD = ../lambdareq/liblambdareq.a ../http/libhttp.a ../net/libnet.a ../util/libutil.a $(OpenSSL_LIBS)

launchser_SOURCES = launchser.cc
launchser_LDADD = ../lambdareq/liblambdareq.a ../http/libhttp.a ../net/libnet.a ../util/libutil.a $(OpenSSL_LIBS)

launchpar_SOURCES = launchpar.cc
launchpar_LDADD = ../lambdareq/liblambdareq.a ../http/libhttp.a ../net/libnet.a ../util/libutil.a $(OpenSSL_LIBS)

launchpipe_SOURCES = launchpipe.cc
launchpipe_LDADD = ../lambdareq/liblambdareq.a ../http/libhttp.a ../net/libnet.a ../util/libutil.a $(OpenSSL_LIBS)
