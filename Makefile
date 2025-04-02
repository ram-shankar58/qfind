CC = gcc
#CFLAGS = -Wall -Wextra -O3 -march=native -pthread -std=c11
#The above produces errors due to strict c11, read more here  https://github.com/microsoft/vscode-cpptools/issues/3547

CFLAGS = -Wall -Wextra -O3 -march=native -pthread -std=gnu11

LDFLAGS = -luring -lzstd -lxxhash -pthread

SRCS = main.c ffbloom.c inverted_index.c io_ops.c search.c index_updates.c qfind.c
OBJS = $(SRCS:.c=.o)
TARGET = qfind

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) -o $@ $^ $(LDFLAGS)

%.o: %.c qfind.h
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)
