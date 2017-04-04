"""
Copyright (c) 2017 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

from subprocess import call
import glob
import os
import logging
from netCDF4 import Dataset, num2date
from springxd.tcpstream import start_server, LengthHeaderTcpProcessor

dimension_order = os.environ['DIMENSION_ORDER']

try:
    output_prefix = os.environ['OUTPUT_PREFIX']
except KeyError:
    output_prefix = 'permuted_'

try:
    permute_variable = os.environ['PERMUTE_VARIABLE']
except KeyError:
    permute_variable = None


def call_ncpdq(self, in_path):
    """
    in_path: Path to input netCDF file

    If environment variable `PERMUTE_VARIABLE` is not set:
        Calls ``ncpdq -a ${DIMENSION_ORDER} in_path ${OUTPUT_PREFIX}in_path``
    Otherwise:
        Calls ``ncpdq -v ${PERMUTE_VARIABLE} -a ${DIMENSION_ORDER} in_path ${OUTPUT_PREFIX}in_path``
    """

    output_filename = output_prefix + os.path.basename(in_path)
    output_path = os.path.join(os.path.dirname(in_path), output_filename)

    command = ['ncpdq', '-a', dimension_order]

    if permute_variable:
        command.append('-v')
        command.append(permute_variable)

    command.append(in_path)
    command.append(output_path)

    logging.debug('Calling command %s' % ' '.join(command))
    call(command)

    yield output_path


def start():
    start_server(call_ncpdq, LengthHeaderTcpProcessor)


if __name__ == "__main__":
    start()
