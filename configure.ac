#                                               -*- Autoconf -*-
# Process this file with ./autogen.sh to produce a configure script

AC_PREREQ([2.68])
AC_INIT([mu], [0.01], [keithw@cs.stanford.edu])
AM_INIT_AUTOMAKE([foreign])
AC_CONFIG_SRCDIR([src/frontend/hello.cc])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIRS([m4])

# Add C++ flags
CXX14_FLAGS="-fPIC -std=c++14 -pthread"
PICKY_CXXFLAGS="-Wall -Wpedantic -Wextra -Weffc++ -Werror"
AC_SUBST([CXX14_FLAGS])
AC_SUBST([PICKY_CXXFLAGS])

# Checks for programs.
AC_PROG_CXX
LT_INIT

# Checks for libraries.
PKG_CHECK_MODULES([OpenSSL], [libssl libcrypto])

# Checks for header files.

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.

AC_CONFIG_FILES([
	Makefile
	src/Makefile
	src/util/Makefile
	src/net/Makefile
	src/http/Makefile
	src/frontend/Makefile
	src/lambdareq/Makefile
	src/launch/Makefile
	src/lambdaize/Makefile
	src/tests/Makefile])

AC_OUTPUT
