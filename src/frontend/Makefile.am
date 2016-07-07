AM_CPPFLAGS = $(CXX14_FLAGS) -I$(srcdir)/../util -I$(srcdir)/../net -I$(srcdir)/../http
AM_CXXFLAGS = $(PICKY_CXXFLAGS)

bin_PROGRAMS = hello

hello_SOURCES = hello.cc
hello_LDADD = ../http/libhttp.a ../net/libnet.a ../util/libutil.a
