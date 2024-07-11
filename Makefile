CC=g++
CXX=g++
RANLIB=ranlib
AR=ar
ARFLAGS=rcs

LIBSRC=MapReduceFramework.cpp Barrier.cpp
LIBOBJ=$(LIBSRC:.cpp=.o)
HEADERS=Barrier.h JobHandler.h

INCS=-I.
CFLAGS = -Wall -pthread -std=c++11 -g $(INCS)
CXXFLAGS = -Wall -pthread -std=c++11 -g $(INCS)

OSMLIB = libMapReduceFramework.a
TARGETS = $(OSMLIB)

TAR=tar
TARFLAGS=-cvf
TARNAME=ex3.tar
TARSRCS=$(LIBSRC) $(HEADERS) Makefile README

# Default target
all: $(TARGETS)

# Create the library
$(TARGETS): $(LIBOBJ)
	$(AR) $(ARFLAGS) $@ $^
	$(RANLIB) $@

# Clean up build files
clean:
	$(RM) $(TARGETS) $(LIBOBJ) *~ *core

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)
