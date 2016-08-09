"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import os
import logging

log = logging.getLogger(__name__)


def module_exists(module_name):
    try:
        __import__(module_name)
    except ImportError:
        return False
    else:
        return True


if module_exists("pyspark"):
    for directory, dirnames, filenames in os.walk(os.path.dirname(__file__)):
        for file in filenames:
            if file != "__init__.py" and (file[-3:] == ".py" or file[-4:] == ".pyx"):
                __import__("algorithms_spark.%s" % file[:file.index(".")])
else:
    log.warn("pyspark not found. Skipping algorithms in %s" % os.path.dirname(__file__))
