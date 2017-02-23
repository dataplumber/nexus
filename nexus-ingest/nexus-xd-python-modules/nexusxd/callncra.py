"""
Copyright (c) 2017 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

from subprocess import call
import glob
import os
from netCDF4 import Dataset, num2date
from springxd.tcpstream import start_server, LengthHeaderTcpProcessor


os.environ['OUTPUT_FILENAME'] = '1x1regrid-ssh_grids_v1609_%Y%m.nc'
os.environ['TIME_VAR_NAME'] = 'Time'


output_filename_pattern = os.environ['OUTPUT_FILENAME']
time_var_name = os.environ['TIME_VAR_NAME']

try:
    glob_pattern = os.environ['FILEMATCH_PATTERN']
except KeyError:
    glob_pattern = '*.nc'



def get_datetime_from_dataset(dataset_path):
    with Dataset(dataset_path) as dataset_in:
        time_units = getattr(dataset_in[time_var_name], 'units', None)
        calendar = getattr(dataset_in[time_var_name], 'calendar', 'standard')
        thedatetime = num2date(dataset_in[time_var_name][:].item(), units=time_units, calendar=calendar)
    return thedatetime


def call_ncra(self, in_path):
    target_datetime = get_datetime_from_dataset(in_path)
    target_yearmonth = target_datetime.strftime('%Y%m')

    output_filename = target_datetime.strftime(output_filename_pattern)
    output_path = os.path.join(os.path.dirname(in_path), output_filename)

    datasets = glob.glob(os.path.join(os.path.dirname(in_path), glob_pattern))

    datasets_to_average = [dataset_path for dataset_path in datasets if
                           get_datetime_from_dataset(dataset_path).strftime('%Y%m') == target_yearmonth]

    command = ['ncra', '-O']
    command.extend(datasets_to_average)
    command.append(output_path)
    call(command)

    yield output_path


def start():
    start_server(call_ncra, LengthHeaderTcpProcessor)


if __name__ == "__main__":

    list(call_ncra(None, '/Users/greguska/data/measures_alt/regrid/1x1regrid-ssh_grids_v1609_1992100212.nc'))
    # start()
