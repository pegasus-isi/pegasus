
class Enum(set):
    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError(name)

    def __getitem__(self, name):
        return self.__getattr__(name)

