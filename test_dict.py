#encoding:UTF-8
class test_dict(dict):
    def __init__(self, names=(), values=(), **kw): #括号是什么用法
        super(test_dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)


adict = test_dict(('a','b','c'),('1','2','3'))
print adict['a']
bdict = test_dict(d = 4, e = 5)
print bdict['d']
