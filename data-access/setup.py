"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import setuptools
import os
import sys
import Cython.Distutils
from distutils.extension import Extension

__version__ = '0.3'


def clean(ext):
    for pyx in ext.sources:
        if pyx.endswith('.pyx'):
            c = pyx[:-4] + '.c'
            cpp = pyx[:-4] + '.cpp'
            so = pyx[:-4] + '.so'
            if os.path.exists(so):
                os.unlink(so)
            if os.path.exists(c):
                os.unlink(c)
            elif os.path.exists(cpp):
                os.unlink(cpp)


extensions = [
    Extension('nexus.data_access.dao.SolrProxy', ["nexus/data_access/dao/SolrProxy.pyx"]),
    Extension('nexus.data_access.dao.CassandraProxy', ["nexus/data_access/dao/CassandraProxy.pyx"])
]

if sys.argv[1] == 'clean':
    print >> sys.stderr, "cleaning .c, .c++ and .so files matching sources"
    map(clean, extensions)

setuptools.setup(
    name="nexus_data_access",
    version=__version__,
    url="https://github.com/dataplumber/nexus",

    author="Team Nexus",

    description="NEXUS API.",
    long_description=open('README.md').read(),

    packages=['nexus', 'nexus.data_access', 'nexus.data_access.model', 'nexus.data_access.dao'],
    package_data={'nexus.data_access.dao': ['config/default-datastores.ini']},
    platforms='any',
    setup_requires=['cython'],
    install_requires=[
        'cassandra-driver==3.5.0',
        'solrpy==0.9.7',
        'nexusproto',
        'shapely'
    ],

    classifiers=[
        'Development Status :: 1 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
    ],
    cmdclass={'build_ext': Cython.Distutils.build_ext},
    ext_modules=extensions
)
