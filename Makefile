############################################
#
# LibPQ Test
#   Copyright 2019.10.05 konoar
#
############################################

TESTBIN := ./testbin
CFLAGS  := -I/usr/local/include
LDFLAGS := -L/usr/local/lib -lpq

.PHONY: clean run

run: $(TESTBIN)
	@./Schema.sh
	@-$(TESTBIN)

clean:
	@-rm *.o
	@-rm $(TESTBIN)

$(TESTBIN): main.o
	@gcc -o $(TESTBIN) $^ $(LDFLAGS)

%.o: %.c
	@gcc -o $@ -c $^ $(CFLAGS)

