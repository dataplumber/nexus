"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import setuptools

__version__ = '0.1'

setuptools.setup(
    name="nexusanalysis",
    version=__version__,
    url="https://github.jpl.nasa.gov/thuang/nexus",

    author="Team Nexus",

    description="NEXUS API.",
    long_description=open('README.md').read(),

    packages=['webservice', 'webservice.algorithms', 'webservice.algorithms_spark'],
    data_files=[
        ('config', ['config/web.ini', 'config/algorithms.ini']),
        ('static', ['static/index.html'])
    ],
    platforms='any',

    install_requires=[
        'nexus-data-access',
        'tornado',
        'singledispatch',
        'pytz',
        'cython',
        'requests',
        'utm',
        'shapely'
    ],

    classifiers=[
        'Development Status :: 1 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
    ]
)
