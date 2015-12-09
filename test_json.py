import json
data = {'name':'ACME','share':100}
json_str = json.dumps(data)
print json_str
data2 = json.loads(json_str)
print data2


class Abc:
    def __init__(self):
        self.name="abc name"
    def jsonable(self):
        return self.name

class Doc:
    def __init__(self):
        self.abc=Abc()
    def jsonable(self):
        return self.__dict__

def ComplexHandler(Obj):
    if hasattr(Obj, 'jsonable'):
        return Obj.jsonable()
    else:
        raise TypeError, 'Object of type %s with value of %s is not JSON serializable' % (type(Obj), repr(Obj))

doc=Doc()
print json.dumps(doc, default=ComplexHandler)
class Identity:
    def __init__(self):
        self.name="abc name"
        self.first="abc first"
        self.addr=Addr()
    def reprJSON(self):
        return dict(name=self.name, firstname=self.first, address=self.addr) 

class Addr:
    def __init__(self):
        self.street="sesame street"
        self.zip="13000"
    def reprJSON(self):
        return dict(street=self.street, zip=self.zip) 

class Doc:
    def __init__(self):
        self.identity=Identity()
        self.data="all data"
    def reprJSON(self):
        return dict(id=self.identity, data=self.data) 

class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj,'reprJSON'):
            return obj.reprJSON()
        else:
            return json.JSONEncoder.default(self, obj)

doc=Doc()
print "Str representation"
print doc.reprJSON()
print "Full JSON"
print json.dumps(doc.reprJSON(), cls=ComplexEncoder)
print "Partial JSON"
print json.dumps(doc.identity.addr.reprJSON(), cls=ComplexEncoder)
