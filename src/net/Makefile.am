AM_CPPFLAGS = $(CXX14_FLAGS) -I$(srcdir)/../util
AM_CXXFLAGS = $(PICKY_CXXFLAGS)

noinst_LTLIBRARIES = libnet.la

libnet_la_SOURCES = file_descriptor.hh file_descriptor.cc \
                   socket.cc socket.hh \
                   address.cc address.hh \
				   secure_socket.cc secure_socket.hh
