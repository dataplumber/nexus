"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import setuptools

__version__ = '0.1'

setuptools.setup(
    name="nexus_matchup_web",
    version=__version__,
    url="https://github.com/dataplumber/nexus",

    author="Team Nexus",

    description="NEXUS HTTP interface to the matchup algorithm.",
    long_description=open('README.md').read(),

    packages=['nexus', 'nexus.analysis'],
    platforms='any',
    install_requires=[
        'tornado',
        'nexus_matchup_algorithm_spark',
        'configargparse'
    ],

    classifiers=[
        'Development Status :: 1 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
    ]
)
