class Factory:
    factories = {}
    def addFactory(id, factory):
        Factory.factories.put[id] = factory
    addFactory = staticmethod(addFactory)
    # A Template Method:
    def createObject(id):
        if not Factory.factories.has_key(id):
            Factory.factories[id] = \
              eval(id + '.Factory()')
        return Factory.factories[id].create()
    createObject = staticmethod(createObject)
