from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

setup(
    cmdclass = {'build_ext': build_ext},
    ext_modules = [Extension("cauchyec", ["cauchyec.pyx","jerasure.c","galois.c","cauchy.c"], extra_compile_args=['-O4']),\
                   Extension("pipe", ["pipe.pyx","jerasure.c","galois.c"], extra_compile_args=['-O4']),\
                   Extension("galoisbuffer", ["galoisbuffer.pyx","jerasure.c","galois.c"], extra_compile_args=['-O4'])])
