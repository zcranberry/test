class ObjectCreator(object):
    pass

print ObjectCreator
print ObjectCreator()

ObjectCreatorMirror = ObjectCreator
print ObjectCreatorMirror
print ObjectCreatorMirror()
MyShinyClass = type('MyShinyClass', (), {})
print MyShinyClass
print MyShinyClass()
