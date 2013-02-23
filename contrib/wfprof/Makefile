CC = gcc
CFLAGS = -Wall
TARGET = wfprof

all: $(TARGET)

wfprof: wfprof.o syscall.o child.o

clean:
	rm -f *.o $(TARGET)
