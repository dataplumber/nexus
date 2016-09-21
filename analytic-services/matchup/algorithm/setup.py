"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import setuptools

__version__ = '0.1'

setuptools.setup(
    name="nexus_matchup_algorithm_spark",
    version=__version__,
    url="https://github.com/dataplumber/nexus",

    author="Team Nexus",

    description="NEXUS algorithm for preforming match ups using Apache Spark.",
    long_description=open('README.md').read(),

    packages=['nexus', 'nexus.analysis'],
    platforms='any',
    install_requires=[
        'nexus_data_access',
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
