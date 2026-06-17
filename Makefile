CC ?= gcc
CFLAGS ?= -O2 -Wall -Wextra -pthread
LDLIBS ?= -lm
TARGET := proxmox_exporter
SOURCE := proxmox_exporter.c

.PHONY: all clean install

all: $(TARGET)

$(TARGET): $(SOURCE)
	$(CC) $(CFLAGS) -o $@ $< $(LDLIBS)

install: $(TARGET)
	install -d /opt/proxmox_exporter
	install -m 0755 $(TARGET) /opt/proxmox_exporter/$(TARGET)
	install -m 0644 $(SOURCE) /opt/proxmox_exporter/$(SOURCE)

clean:
	rm -f $(TARGET)
