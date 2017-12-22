AM_CPPFLAGS = $(CXX14_FLAGS) -I$(srcdir)/../util -I$(srcdir)/../net -I$(srcdir)/../http -I$(srcdir)/../lambdareq
AM_CXXFLAGS = $(PICKY_CXXFLAGS)

# and if we want to make a seperate executable...
bin_PROGRAMS = launcher
launcher_SOURCES = launch.hh launch.cc concurrentqueue.h blockingconcurrentqueue.h launcher.cc
nodist_launcher_SOURCES = launch.pb.h launch.pb.cc launch.grpc.pb.h launch.grpc.pb.cc
launcher_LDADD = ../lambdareq/liblambdareq.la ../http/libhttp.la ../net/libnet.la ../util/libutil.la $(OpenSSL_LIBS)
launcher_LDFLAGS = -lprotobuf -lgrpc++ -lpthread

launcher_CPPFLAGS = $(AM_CPPFLAGS)

lib_LTLIBRARIES = liblaunch.la
liblaunch_la_SOURCES = launch.hh launch.cc concurrentqueue.h blockingconcurrentqueue.h
nodist_liblaunch_la_SOURCES = launch.pb.h launch.pb.cc launch.grpc.pb.h launch.grpc.pb.cc
liblaunch_la_LIBADD = ../lambdareq/liblambdareq.la ../http/libhttp.la ../net/libnet.la ../util/libutil.la $(OpenSSL_LIBS)
liblaunch_la_LDFLAGS = -lprotobuf -lgrpc++ -lpthread

MOSTLYCLEANFILES = launch.pb.h launch.pb.cc launch.grpc.pb.h launch.grpc.pb.cc


