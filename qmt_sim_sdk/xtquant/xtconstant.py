from ..xtquant_compat import xtconstant as _constants

for _name in dir(_constants):
    if not _name.startswith("_"):
        globals()[_name] = getattr(_constants, _name)
del _name

