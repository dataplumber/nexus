"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import os

for file in os.listdir(os.path.dirname(__file__)):
    if file != "__init__.py" and (file[-3:] == ".py" or file[-4:] == ".pyx"):
        __import__("algorithms.%s"%file[:file.index(".")])