# See http://stackoverflow.com/questions/1675734/how-do-i-create-a-namespace-package-in-python
from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)

