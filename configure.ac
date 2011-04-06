# This file is used for Roomy development, only -- not for making Roomy.
# Process this file with autoconf to produce a configure script.

# Init Autoconf and Automake
AC_INIT([Roomy], [0.9.1], [Dan Kunkle <kunkle@gmail.com>], [roomy],
        [http://roomy.sourceforge.net/])
AM_INIT_AUTOMAKE([-Wall -Werror foreign])

# Check for programs.
AC_PROG_LN_S
AC_PROG_RANLIB
AC_PROG_INSTALL
AC_PROG_MAKE_SET

# Check for libraries.
AC_CHECK_LIB(pthread, pthread_create)
AC_CHECK_LIB(mpich, MPI_Init_thread)

# Check for mpicc.
AC_CHECK_PROG(CC, mpicc, mpicc)

# Files to generate.
AC_CONFIG_FILES([
Makefile
src/Makefile
src/tests/Makefile
])
AC_OUTPUT

