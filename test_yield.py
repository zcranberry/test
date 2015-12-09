def fib(max):
    n, a, b = 0, 0, 1
    while n < max:
        yield b
        a, b, n = b, a + b, n + 1
    #return 'done'


f = fib(6)
for n in f:
    print n
