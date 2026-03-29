CC = gcc
CFLAGS = -std=c11 -Wall -Wextra -O2 -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE
TARGET = native-wimage
OPENSSL_FOUND := $(shell pkg-config --exists openssl && echo 1 || echo 0)

ifeq ($(OPENSSL_FOUND),1)
OPENSSL_CFLAGS := $(shell pkg-config --cflags openssl)
OPENSSL_LIBS := $(shell pkg-config --libs openssl)
CFLAGS += $(OPENSSL_CFLAGS) -DHAVE_OPENSSL_SHA1
endif

LIB_DIR = libwim
LIB_SRCS = $(LIB_DIR)/sha1.c $(LIB_DIR)/xpress_huff.c $(LIB_DIR)/wim_io.c \
           $(LIB_DIR)/wim_read.c $(LIB_DIR)/wim_write.c $(LIB_DIR)/wim_capture.c
LIB_OBJS = $(LIB_SRCS:.c=.o)
LIB_A = $(LIB_DIR)/libwim.a

TOOL_SRCS = wimage.c
TOOL_OBJS = $(TOOL_SRCS:.c=.o)

all: $(TARGET)

$(LIB_A): $(LIB_OBJS)
	ar rcs $@ $^

$(TARGET): $(TOOL_OBJS) $(LIB_A)
	$(CC) $(CFLAGS) -o $@ $(TOOL_OBJS) $(LIB_A) -lpthread $(OPENSSL_LIBS)

$(LIB_DIR)/%.o: $(LIB_DIR)/%.c
	$(CC) $(CFLAGS) -I$(LIB_DIR) -c $< -o $@

%.o: %.c
	$(CC) $(CFLAGS) -I$(LIB_DIR) -c $< -o $@

clean:
	rm -f $(LIB_OBJS) $(TOOL_OBJS) $(LIB_A) $(TARGET)

.PHONY: all clean
