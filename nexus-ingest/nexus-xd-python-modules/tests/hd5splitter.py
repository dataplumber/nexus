"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

# from netCDF4 import Dataset

from h5py import File, Dataset

hinput = File(
    '/Users/greguska/gitprojeects/nexus/nexus-ingest/developer-box/data/smap/SMAP_L2B_SSS_00865_20150331T163144_R13080.h5',
    'r')
houput = File(
    '/Users/greguska/gitprojeects/nexus/nexus-ingest/developer-box/data/smap/SMAP_L2B_SSS_00865_20150331T163144_R13080.split.h5',
    'w')

for key in hinput.keys():
    hinput.copy('/' + key, houput['/'], name=key)
    if houput[key].ndim == 2:
        houput[key + '_c'] = houput[key][0:76, 181:183]
    elif houput[key].ndim == 3:
        houput[key + '_c'] = houput[key][0:76, 181:183, :]
    elif houput[key].ndim == 1:
        houput[key + '_c'] = houput[key][181:183]

    houput[key + '_c'].attrs.update(houput[key].attrs)
    del houput[key]
    houput[key] = houput[key + '_c']
    del houput[key + '_c']

    print houput[key]

houput.attrs.update(hinput.attrs)

houput.flush()

print hinput['/']
print houput['/']

print [attr for attr in hinput.attrs]
print [attr for attr in houput.attrs]

hinput.close()
houput.close()
