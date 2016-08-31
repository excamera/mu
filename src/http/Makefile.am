AM_CPPFLAGS = $(CXX14_FLAGS) -I$(srcdir)/../util
AM_CXXFLAGS = $(PICKY_CXXFLAGS)

noinst_LTLIBRARIES = libhttp.la

libhttp_la_SOURCES = http_request_parser.hh \
        http_request.hh http_request.cc \
        http_response.hh http_response.cc \
        http_header.hh http_header.cc \
        http_response_parser.hh http_response_parser.cc \
        tokenize.hh mime_type.hh mime_type.cc \
        body_parser.hh \
        chunked_parser.hh chunked_parser.cc \
        http_message.hh http_message.cc \
        http_message_sequence.hh
