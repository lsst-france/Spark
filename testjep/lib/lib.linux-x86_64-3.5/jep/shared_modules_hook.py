# This provides support for sharing Python modules amongst different Python
# sub-interpreters. This can be used to enable modules which do not work well
# in sub-interpreters to be used within Jep. Although this capability was
# designed specifically to accommodate numpy the same functionality should work
# with any Python module that has similar problems with sub-interpreters.
#
# The problem with using numpy with sub-interpreters is that numpy will cache
# methods in static variables in C. Many of these variables are set up during
# during the initial numpy import. The most obvious problem with this is that
# there can only be one cached version of the method and there can be multiple
# instances of the numpy module, one in each sub-interpreter. This means
# invoking methods from numpy in one sub-interpreter might actually be using
# methods defined in another sub-interpreter. To make things even more
# interesting, some methods are not cached during the intitial import but are
# cached when they are first used. If there are a variety of Jep threads using
# different numpy methods it can result in each cached method coming from a
# different sub-interpreter. Although this is a very odd state of things, it
# usually isn't a noticeable problem since all the different instances of numpy
# are defined the same and they do the same thing.
#
# The problem becomes noticeable when the sub-interpreter that has methods
# cached in static variables in C is disposed. The methods themselves are not
# affected, since the refcount reflects that they are still in use but almost
# all of the cached methods are part of a module and they will use variables
# defined at the module level (such as import statements at the top of the
# file). When a sub-interpreter is closed Python cleans up modules by setting
# all the module variables to null so when the cached method attempts to use
# module level variables then errors will occur. For example if the cached
# method calls another method defined in the module it will result in the
# infamous 'NoneType is not callable'. To solve this problem, a variety of
# solutions were considered, each with their own strengths and weaknesses.
#
# One possible solution is to update numpy to never cache Python objects in C
# static variables. Some of the methods that are kept in C are configurable
# objects (like the ndarray toString method), these methods would have to be
# stored as part of a Python module instead of a C static variable. The other
# reason things are stored in C static variables is for performance
# optimization, just to save the time it takes to look up the correct method.
# In this case the only way to be compatible with sub-interpreters would be to
# abandon this optimization. Since the numpy project is specifically written to
# achieve high performance it does not seem in their best interest to slow
# things down to support sub-interpreters.
#
# The second possible solution within numpy would be to never use module level
# variables from methods that are cached in C. The simplest way to achieve this
# would be to wrap the method in a closure that contains all the variables
# necessary. While this is a technically solid solution, it makes the code
# hideously cluttered. It also affects much more than the methods that are
# directly cached, i.e. any methods that the cached methods call must also be
# wrapped in a closure and the end result is that the entire numpy source would
# be prohibited from using module level variables, something that most Python
# developers do without even thinking. The final nail in the coffin for this
# solution is that it doesn't even solve the real problem, numpy will still be
# calling methods that belong in dead interpreters.
#
# The most obvious solution for Jep, and the de facto solution in the Python
# community is to just stop using sub-interpreters. It would be possible to use
# different global dictionaries to somewhat isolate different Jep instances
# that are all sharing the same interpreter, essentially the only thing that
# Jep instances would share is any imported modules. This solution immediately
# falls apart because different Jep instances have different local import
# paths. Since this path is stored in the sys.path variable there is no way to
# achieve this functionality while the sys module is shared.
#
# Finally, the solution defined in this module, the shared module importer. It
# is a compromise that allows sys and most other Python modules to be isolated
# in sub-interpreters but a single instance of the numpy module can be shared
# by all Jep instances. It still has the oddity that numpy is used outside of
# the interpreter which created it but it at least introduces determinism. It
# is a specific, known, interpreter that creates the module and it is
# guaranteed that it will not be disposed.
#

import sys


class JepSharedModuleImporter(object):

    def __init__(self, moduleList, sharedImporter):
        self.moduleList = list(moduleList)
        self.sharedImporter = sharedImporter
        if sys.version_info.major <= 2:
            # Python 2 will deadlock if you attempt to switch threads and import
            # during an import so instead preemptively import everything shared
            # https://docs.python.org/2/library/threading.html#threaded-imports
            from _jep import topInterpreterModules
            for module in moduleList:
                if module not in topInterpreterModules:
                    self.sharedImporter.sharedImport(module)

    def find_module(self, fullname, path=None):
        if fullname in self.moduleList:
            return self
        for moduleName in self.moduleList:
            if fullname.startswith(moduleName + "."):
                return self
        return None

    def load_module(self, fullname):
        if fullname not in sys.modules:
            from _jep import topInterpreterModules, topInterpreterModulesLock
            with topInterpreterModulesLock:
                if fullname not in topInterpreterModules and sys.version_info.major > 2:
                    self.sharedImporter.sharedImport(fullname)
                # Must copy all modules or relative imports will be broken
                for moduleName in self.moduleList:
                    for key in topInterpreterModules:
                        if key == moduleName or key.startswith(moduleName + "."):
                            sys.modules[key] = topInterpreterModules[key]
        return sys.modules[fullname]

    def unload_modules(self):
        for moduleName in self.moduleList:
            for key in set(sys.modules):
                if key == moduleName or key.startswith(moduleName + "."):
                    del sys.modules[key]


def setupImporter(moduleList, sharedImporter):
    for importer in sys.meta_path:
        if isinstance(importer, JepSharedModuleImporter):
            return
    sys.meta_path.insert(0,JepSharedModuleImporter(moduleList, sharedImporter))


def teardownImporter():
    for importer in sys.meta_path:
        if isinstance(importer, JepSharedModuleImporter):
            importer.unload_modules()
