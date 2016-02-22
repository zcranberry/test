import sys
def foo():
    try:
        a = 1 / 0
        print 'foo'
    except Exception:
        sys.exit('abcdefg ') # 可以直接打在屏幕上

def bar():
    foo()
    print 'bar'


if __name__ == '__main__':
    bar()
