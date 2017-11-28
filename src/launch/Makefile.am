AM_CPPFLAGS = $(CXX14_FLAGS) -I$(srcdir)/../util -I$(srcdir)/../net -I$(srcdir)/../http -I$(srcdir)/../lambdareq
AM_CXXFLAGS = $(PICKY_CXXFLAGS)

%.pb.cc %.pb.h: %.proto
		@protoc -I$(srcdir) --grpc_out=$(builddir) --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` $(srcdir)/launch.proto
		@protoc -I$(srcdir) --cpp_out=$(builddir) $(srcdir)/launch.proto
		@python -m grpc_tools.protoc -I$(srcdir) --python_out=$(builddir)/../lambdaize/libmu/ --grpc_python_out=$(builddir)/../lambdaize/libmu/ $(srcdir)/launch.proto

lib_LTLIBRARIES = liblaunch.la
liblaunch_la_SOURCES = launch.hh launch.cc concurrentqueue.h blockingconcurrentqueue.h
nodist_liblaunch_la_SOURCES = $(builddir)/launch.pb.h $(builddir)/launch.pb.cc $(builddir)/launch.grpc.pb.h $(builddir)/launch.grpc.pb.cc
liblaunch_la_LIBADD = ../lambdareq/liblambdareq.la ../http/libhttp.la ../net/libnet.la ../util/libutil.la $(OpenSSL_LIBS)
liblaunch_la_LDFLAGS = -lprotobuf -lgrpc++ -lpthread

MOSTLYCLEANFILES = launch.pb.h launch.pb.cc launch.grpc.pb.h launch.grpc.pb.cc

#bin_PROGRAMS = cpplaunch

#cpplaunch_SOURCES = launch.hh launch.cc launch.pb.h launch.pb.cc launch.grpc.pb.h launch.grpc.pb.cc
#cpplaunch_LDADD = ../lambdareq/liblambdareq.la ../http/libhttp.la ../net/libnet.la ../util/libutil.la $(OpenSSL_LIBS)
#cpplaunch_LDFLAGS = -lprotobuf -lgrpc++ -lpthread
