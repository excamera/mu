AM_CPPFLAGS = $(CXX14_FLAGS) \
			  -I$(srcdir)/../util -I$(srcdir)/../net \
			  -I$(srcdir)/../http -I$(srcdir)/../lambdareq \
			  -I$(srcdir)/../launch
AM_CXXFLAGS = $(PICKY_CXXFLAGS)

bin_PROGRAMS = hello launchser launchpar launchpipe

hello_SOURCES = hello.cc
hello_LDADD = ../lambdareq/liblambdareq.la ../http/libhttp.la ../net/libnet.la ../util/libutil.la $(OpenSSL_LIBS)

launchser_SOURCES = launchser.cc
launchser_LDADD = ../lambdareq/liblambdareq.la ../http/libhttp.la ../net/libnet.la ../util/libutil.la $(OpenSSL_LIBS)

launchpar_SOURCES = launchpar.cc
launchpar_LDADD = ../launch/liblaunch.la $(OpenSSL_LIBS)
launchpar_LDFLAGS = $(AM_LDFLAGS) -static

launchpipe_SOURCES = launchpipe.cc
launchpipe_LDADD = ../lambdareq/liblambdareq.la ../http/libhttp.la ../net/libnet.la ../util/libutil.la $(OpenSSL_LIBS)
