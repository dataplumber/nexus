"""
Copyright (c) 2017 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

from subprocess import call
import glob
import os
from netCDF4 import Dataset, num2date
from springxd.tcpstream import start_server, LengthHeaderTcpProcessor

output_filename = os.environ['OUTPUT_FILENAME']
time_var_name = os.environ['TIME']


def get_month_from_dataset(dataset_path):
    with Dataset(dataset_path) as dataset_in:
        time_units = getattr(dataset_in[time_var_name], 'units', None)
        calendar = getattr(dataset_in[time_var_name], 'calendar', 'standard')
        month = num2date(dataset_in[time_var_name][:].item(), units=time_units, calendar=calendar).strftime('%m')
    return month


def call_ncra(self, in_path):
    output_path = os.path.join(os.path.dirname(in_path), output_filename)
    target_month = get_month_from_dataset(in_path)

    datasets = glob.glob(os.path.join(os.path.dirname(in_path), '*.nc'))

    datasets_to_average = [dataset_path for dataset_path in datasets if
                           get_month_from_dataset(dataset_path) == target_month]

    command = ['ncra']
    command.extend(datasets_to_average)
    command.append(output_path)
    call(command)

    yield output_path


def start():
    start_server(call_ncra, LengthHeaderTcpProcessor)


if __name__ == "__main__":
    start()
