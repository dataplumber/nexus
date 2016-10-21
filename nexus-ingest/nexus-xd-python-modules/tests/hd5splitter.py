"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""


def hd5_copy(source, dest):
    for key in source.keys():
        source.copy('/' + key, dest['/'], name=key)

        print key

        if str(key) == 'time':
            dest[key + '_c'] = dest[key][0:4]
        elif str(key) == 'longitude':
            dest[key + '_c'] = dest[key][0:87]
        elif str(key) == 'latitude':
            dest[key + '_c'] = dest[key][0:38]
        else:
            dest[key + '_c'] = dest[key][0:4, 0:38, 0:87]

        # Useful for swath data:
        # if dest[key].ndim == 2:
        #     dest[key + '_c'] = dest[key][0:76, 181:183]
        # elif dest[key].ndim == 3:
        #     dest[key + '_c'] = dest[key][0:76, 181:183, :]
        # elif dest[key].ndim == 1:
        #     dest[key + '_c'] = dest[key][181:183]

        for att in dest[key + '_c'].attrs:
            try:
                dest[key + '_c'].attrs.modify(dest[key].attrs.get(att, default=""))
            except IOError:
                print "error " + att
                pass
        dest[key + '_c'].attrs.update(dest[key].attrs)
        del dest[key]
        dest[key] = dest[key + '_c']
        del dest[key + '_c']

        print dest[key]

    for att in dest.attrs:
        try:
            dest.attrs.modify(source.attrs.get(att, default=""))
        except IOError:
            print "error " + att
            pass

    # dest.attrs.update(source.attrs)

    dest.flush()


def netcdf_subset(source, dest):
    dtime = dest.createDimension(dimname='time', size=4)
    dlat = dest.createDimension(dimname='latitude', size=38)
    dlon = dest.createDimension(dimname='longitude', size=87)

    dest.setncatts(source.__dict__)

    for variable in source.variables:
        variable = source[variable]

        if variable.name == 'time':
            dvar = dest.createVariable(varname=variable.name, datatype=variable.dtype, dimensions=(dtime.name,))
            dvar[:] = variable[0:4]
        elif variable.name == 'longitude':
            dvar = dest.createVariable(varname=variable.name, datatype=variable.dtype, dimensions=(dlon.name,))
            dvar[:] = variable[0:87]
        elif variable.name == 'latitude':
            dvar = dest.createVariable(varname=variable.name, datatype=variable.dtype, dimensions=(dlat.name,))
            dvar[:] = variable[0:38]
        else:
            dvar = dest.createVariable(varname=variable.name, datatype=variable.dtype,
                                       dimensions=(dtime.name, dlat.name, dlon.name))
            dvar[:] = variable[0:4, 0:38, 0:87]

        dest[variable.name].setncatts(variable.__dict__)

    dest.sync()
    dest.close()


from netCDF4 import Dataset

hinput = Dataset(
    '/Users/greguska/githubprojects/nexus/nexus-ingest/developer-box/data/ccmp/CCMP_Wind_Analysis_20160101_V02.0_L3.0_RSS.nc',
    'r')
houtput = Dataset(
    '/Users/greguska/githubprojects/nexus/nexus-ingest/developer-box/data/ccmp/CCMP_Wind_Analysis_20160101_V02.0_L3.0_RSS.split.nc',
    mode='w')

netcdf_subset(hinput, houtput)

# # from h5py import File, Dataset
# hinput = File(
#     '/Users/greguska/githubprojects/nexus/nexus-ingest/developer-box/data/ccmp/CCMP_Wind_Analysis_20160101_V02.0_L3.0_RSS.nc',
#     'r')
# houput = File(
#     '/Users/greguska/githubprojects/nexus/nexus-ingest/developer-box/data/ccmp/CCMP_Wind_Analysis_20160101_V02.0_L3.0_RSS.split.nc',
#     'w')

# hd5_copy(hinput, houput)

# print hinput['/']
# print houtput['/']

# print [attr for attr in hinput.attrs]
# print [attr for attr in houtput.attrs]

# hinput.close()
# houtput.close()
