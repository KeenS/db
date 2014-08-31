CFLAGS = -std=c99 -ggdb

default: all

.PHONY: all default test clean

all: file.o test-file.o datadef.o test-datadef.o datamanip.o test-datamanip.o microdb.h

test: test-file test-datadef test-datamanip
	./test-file
	./test-datadef
	./test-datamanip

test-file: file.o test-file.o
	$(CC) -o test-file $(CFLAGS) test-file.o file.o

test-datadef: datadef.o test-datadef.o datamanip.o file.o
	$(CC) -o test-datadef $(CFLAGS) test-datadef.o datadef.o datamanip.o file.o

test-datamanip: datamanip.o test-datadef.o datadef.o file.o
	$(CC) -o test-datamanip $(CFLAGS) test-datamanip.o datadef.o datamanip.o file.o
%.o: %.c microdb.h
	$(CC) -o $@ $(CFLAGS) -c $<

clean:
	rm *.o
