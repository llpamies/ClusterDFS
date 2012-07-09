from distutils.core import setup
from distutils.extension import Extension

try:
    from Cython.Distutils import build_ext
except ImportError:
    use_cython = False
else:
    use_cython = True

cmdclass = {}
ext_modules = []

if use_cython:
    ext_modules += [
        Extension("cauchyec", ["cauchyec.pyx","jerasure.c","galois.c","cauchy.c"], extra_compile_args=['-O4']),
        Extension("galoisbuffer", ["galoisbuffer.pyx","jerasure.c","galois.c"], extra_compile_args=['-O4'])
    ]
    cmdclass.update({ 'build_ext': build_ext })
else:
    ext_modules += [
        Extension("cauchyec", ["cauchyec.c","jerasure.c","galois.c","cauchy.c"], extra_compile_args=['-O4']),
        Extension("galoisbuffer", ["galoisbuffer.c","jerasure.c","galois.c"], extra_compile_args=['-O4'])
    ]

setup(cmdclass=cmdclass, ext_modules=ext_modules)
