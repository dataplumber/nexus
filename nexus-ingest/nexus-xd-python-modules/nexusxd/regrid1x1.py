"""
Copyright (c) 2017 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

from datetime import datetime

import numpy as np
from netCDF4 import Dataset
from pytz import timezone
from scipy import interpolate
from springxd.tcpstream import start_server, LengthHeaderTcpProcessor
import os

UTC = timezone('UTC')
ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'

variables_to_regrid = os.environ['REGRID_VARIABLES']
latitude_var_name = os.environ['LATITUDE']
longitude_var_name = os.environ['LONGITUDE']
time_var_name = os.environ['TIME']

try:
    filename_prefix = os.environ['FILENAME_PREFIX']
except KeyError:
    filename_prefix = '1x1regrid-'

try:
    vvr = os.environ['VARIABLE_VALID_RANGE']
    vvr_iter = iter(vvr.split(':'))
    variable_valid_range = {varrange[0]: [varrange[1], varrange[2]] for varrange in
                            zip(vvr_iter, vvr_iter, vvr_iter)}
except KeyError:
    variable_valid_range = {}


def regrid(self, in_filepath):
    in_path = os.path.join('/', *in_filepath.split(os.sep)[0:-1])
    out_filepath = os.path.join(in_path, filename_prefix + in_filepath.split(os.sep)[-1])

    with Dataset(in_filepath) as inputds:
        in_lon = inputds[longitude_var_name]
        in_lat = inputds[latitude_var_name]

        lon1deg = np.arange(np.floor(np.min(in_lon)), np.ceil(np.max(in_lon)), 1)
        lat1deg = np.arange(np.floor(np.min(in_lat)), np.ceil(np.max(in_lat)), 1)

        with Dataset(out_filepath, mode='w') as outputds:
            outputds.createDimension(longitude_var_name, len(lon1deg))
            outputds.createVariable(longitude_var_name, in_lon.dtype, dimensions=(longitude_var_name,))
            outputds[longitude_var_name][:] = lon1deg
            outputds[longitude_var_name].setncatts(
                {attrname: inputds[longitude_var_name].getncattr(attrname) for attrname in
                 inputds[longitude_var_name].ncattrs() if str(attrname) != 'bounds'})

            outputds.createDimension(latitude_var_name, len(lat1deg))
            outputds.createVariable(latitude_var_name, in_lat.dtype, dimensions=(latitude_var_name,))
            outputds[latitude_var_name][:] = lat1deg
            outputds[latitude_var_name].setncatts(
                {attrname: inputds[latitude_var_name].getncattr(attrname) for attrname in
                 inputds[latitude_var_name].ncattrs() if str(attrname) != 'bounds'})

            outputds.createDimension(time_var_name)
            outputds.createVariable(time_var_name, inputds[time_var_name].dtype, dimensions=(time_var_name,))
            outputds[time_var_name][:] = inputds[time_var_name][:]
            outputds[time_var_name].setncatts(
                {attrname: inputds[time_var_name].getncattr(attrname) for attrname in inputds[time_var_name].ncattrs()
                 if
                 str(attrname) != 'bounds'})

            for variable_name in variables_to_regrid.split(','):
                in_data = inputds[variable_name][:]
                in_data = np.squeeze(in_data)
                np.ma.set_fill_value(in_data, float('NaN'))
                in_data = in_data.T

                # Produces erroneous values on the edges of data
                # interp_func = interpolate.interp2d(in_lon[:], in_lat[:], in_data[:], fill_value=float('NaN'))

                x_mesh, y_mesh = np.meshgrid(in_lon[:], in_lat[:], copy=False)

                # Does not work for large datasets (n > 5000)
                # interp_func = interpolate.Rbf(x_mesh, y_mesh, in_data[:], function='linear', smooth=0)

                x1_mesh, y1_mesh = np.meshgrid(lon1deg, lat1deg, copy=False)
                out_data = interpolate.griddata(np.array([x_mesh.ravel(), y_mesh.ravel()]).T, in_data.ravel(),
                                                (x1_mesh, y1_mesh), method='nearest').T

                outputds.createVariable(variable_name, inputds[variable_name].dtype,
                                        dimensions=inputds[variable_name].dimensions)
                outputds[variable_name].setncatts(
                    {attrname: inputds[variable_name].getncattr(attrname) for attrname in
                     inputds[variable_name].ncattrs()})
                if variable_name in variable_valid_range.keys():
                    outputds[variable_name].valid_range = [
                        np.array([variable_valid_range[variable_name][0]], dtype=inputds[variable_name].dtype).item(),
                        np.array([variable_valid_range[variable_name][1]], dtype=inputds[variable_name].dtype).item()]
                outputds[variable_name][:] = out_data[np.newaxis, :]

            global_atts = {
                'geospatial_lon_min': np.float(np.min(lon1deg)),
                'geospatial_lon_max': np.float(np.max(lon1deg)),
                'geospatial_lat_min': np.float(np.min(lat1deg)),
                'geospatial_lat_max': np.float(np.max(lat1deg)),
                'Conventions': 'CF-1.6',
                'date_created': datetime.utcnow().replace(tzinfo=UTC).strftime(ISO_8601),
                'title': getattr(inputds, 'title', None),
                'time_coverage_start': getattr(inputds, 'time_coverage_start', None),
                'time_coverage_end': getattr(inputds, 'time_coverage_end', None),
                'Institution': getattr(inputds, 'Institution', None),
                'summary': getattr(inputds, 'summary', None),
            }

            outputds.setncatts(global_atts)

    yield out_filepath


def start():
    start_server(regrid, LengthHeaderTcpProcessor)


if __name__ == "__main__":
    start()
