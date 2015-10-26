import time, uuid

def next_id(t=None):
    '''
    Return next id as 50-char string.

    Args:
        t: unix timestamp, default to None and using time.time().
    '''
    if t is None:
        t = time.time()
    return '%015d%s000' % (int(t * 1000), uuid.uuid4().hex)

print next_id()
print next_id(1)
print next_id(1)

class private_test:
    __a__ = 1
    __a = 2
    _a = 3

o = private_test()
print o.__a__
print o.__a
print o._a

