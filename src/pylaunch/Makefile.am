AM_CPPFLAGS = $(CXX14_FLAGS) $(PYTHON_INCLUDES) -I$(srcdir)/../launch
AM_CXXFLAGS = $(PICKY_CXXFLAGS)

pyexec_LTLIBRARIES = pylaunch.la
pylaunch_la_SOURCES = pylaunchmodule.cc
pylaunch_la_LIBADD = ../launch/liblaunch.la
pylaunch_la_LDFLAGS = -module -avoid-version -export-symbols-regex initpylaunch -shared
