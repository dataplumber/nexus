import sys
import numpy as np
from time import time
from webservice.NexusHandler import NexusHandler, nexus_handler, DEFAULT_PARAMETERS_SPEC
from netCDF4 import Dataset

class SparkAlg(NexusHandler):

    def __init__(self):
        NexusHandler.__init__(self, skipCassandra=False, skipSolr=False)

    def _setQueryParams(self, ds, bounds, start_time=None, end_time=None,
                        start_year=None, end_year=None, clim_month=None,
                        fill=-9999.):
        self._ds = ds
        self._minLat, self._maxLat, self._minLon, self._maxLon = bounds
        self._startTime = start_time
        self._endTime = end_time
        self._startYear = start_year
        self._endYear = end_year
        self._climMonth = clim_month
        self._fill = fill

    def _find_native_resolution(self):
        if type(self._ds) in (list,tuple):
            ds = self._ds[0]
        else:
            ds = self._ds
        ntiles = 0
        t_incr = 86400
        t = self._endTime
        while ntiles == 0:
            nexus_tiles = self.query_by_parts(self._tile_service,
                                              self._minLat, self._maxLat, 
                                              self._minLon, self._maxLon,
                                              ds, t-t_incr, t)
            ntiles = len(nexus_tiles)
            print 'find_native_res: got %d tiles' % len(nexus_tiles)
            sys.stdout.flush()
            lat_res = 0.
            lon_res = 0.
            if ntiles > 0:
                for tile in nexus_tiles:
                    print 'tile coords:'
                    print 'tile lats: ', tile.latitudes
                    print 'tile lons: ', tile.longitudes
                    if lat_res < 1e-10:
                        lats = tile.latitudes.compressed()
                        if (len(lats) > 1):
                            lat_res = lats[1] - lats[0]
                    if lon_res < 1e-10:
                        lons = tile.longitudes.compressed()
                        if (len(lons) > 1):
                            lon_res = lons[1] - lons[0]
                    if (lat_res >= 1e-10) and (lon_res >= 1e-10):
                        break
            if (lat_res < 1e-10) or (lon_res < 1e-10):
                t -= t_incr

        self._latRes = lat_res
        self._lonRes = lon_res

    def _find_global_tile_set(self):
        if type(self._ds) in (list,tuple):
            ds = self._ds[0]
        else:
            ds = self._ds
        ntiles = 0
        t = self._endTime
        t_incr = 86400
        while ntiles == 0:
            nexus_tiles = self._tile_service.get_tiles_bounded_by_box(self._minLat, self._maxLat, self._minLon, self._maxLon, ds=ds, start_time=t-t_incr, end_time=t)
            ntiles = len(nexus_tiles)
            print 'find_global_tile_set got %d tiles' % ntiles
            sys.stdout.flush()
            t -= t_incr
        return nexus_tiles

    def _find_tile_bounds(self, t):
        lats = t.latitudes
        lons = t.longitudes
        #print 'lats=',lats
        #print 'lons=',lons
        if (len(lats.compressed()) > 0) and (len(lons.compressed()) > 0):
            min_lat = np.ma.min(lats)
            max_lat = np.ma.max(lats)
            min_lon = np.ma.min(lons)
            max_lon = np.ma.max(lons)
            good_inds_lat = np.where(lats.mask == False)[0]
            good_inds_lon = np.where(lons.mask == False)[0]
            min_y = np.min(good_inds_lat)
            max_y = np.max(good_inds_lat)
            min_x = np.min(good_inds_lon)
            max_x = np.max(good_inds_lon)
            bounds = (min_lat, max_lat, min_lon, max_lon,
                      min_y, max_y, min_x, max_x)
        else:
            print '*****************************Nothing in this tile!'
            bounds = None
        return bounds
        
    @staticmethod
    def query_by_parts(tile_service, min_lat, max_lat, min_lon, max_lon, 
                       dataset, start_time, end_time, part_dim=0):
        nexus_max_tiles_per_query = 100
        print 'trying query: ',min_lat, max_lat, min_lon, max_lon, \
            dataset, start_time, end_time
        try:
            tiles = \
                tile_service.find_tiles_in_box(min_lat, max_lat, 
                                               min_lon, max_lon, 
                                               dataset, 
                                               start_time=start_time, 
                                               end_time=end_time,
                                               fetch_data=False)
            assert(len(tiles) <= nexus_max_tiles_per_query)
        except:
            print 'failed query: ',min_lat, max_lat, min_lon, max_lon, \
                dataset, start_time, end_time
            if part_dim == 0: 
                # Partition by latitude.
                mid_lat = (min_lat + max_lat) / 2
                nexus_tiles = SparkAlg.query_by_parts(tile_service, 
                                                      min_lat, mid_lat, 
                                                      min_lon, max_lon, 
                                                      dataset, 
                                                      start_time, end_time,
                                                      part_dim = part_dim)
                nexus_tiles.extend(SparkAlg.query_by_parts(tile_service, 
                                                           mid_lat, max_lat, 
                                                           min_lon, max_lon, 
                                                           dataset, 
                                                           start_time, 
                                                           end_time,
                                                           part_dim = part_dim))
            elif part_dim == 1: 
                # Partition by longitude.
                mid_lon = (min_lon + max_lon) / 2
                nexus_tiles = SparkAlg.query_by_parts(tile_service, 
                                                      min_lat, max_lat, 
                                                      min_lon, mid_lon, 
                                                      dataset, 
                                                      start_time, end_time,
                                                      part_dim = part_dim)
                nexus_tiles.extend(SparkAlg.query_by_parts(tile_service, 
                                                           min_lat, max_lat, 
                                                           mid_lon, max_lon, 
                                                           dataset, 
                                                           start_time, 
                                                           end_time,
                                                           part_dim = part_dim))
            elif part_dim == 2:
                # Partition by time.
                mid_time = (start_time + end_time) / 2
                nexus_tiles = SparkAlg.query_by_parts(tile_service, 
                                                      min_lat, max_lat, 
                                                      min_lon, max_lon, 
                                                      dataset, 
                                                      start_time, mid_time,
                                                      part_dim = part_dim)
                nexus_tiles.extend(SparkAlg.query_by_parts(tile_service, 
                                                           min_lat, max_lat, 
                                                           min_lon, max_lon, 
                                                           dataset, 
                                                           mid_time, 
                                                           end_time,
                                                           part_dim = part_dim))
        else:
            # No exception, so query Cassandra for the tile data.
            print 'Making NEXUS query to Cassandra for %d tiles...' % \
                len(tiles)
            t1 = time()
            print 'NEXUS call start at time %f' % t1
            sys.stdout.flush()
            nexus_tiles = list(tile_service.fetch_data_for_tiles(*tiles))
            nexus_tiles = list(tile_service.mask_tiles_to_bbox(min_lat, max_lat,
                                                               min_lon, max_lon,
                                                               nexus_tiles))
            t2 = time()
            print 'NEXUS call end at time %f' % t2
            print 'Seconds in NEXUS call: ', t2-t1
            sys.stdout.flush()

        print 'Returning %d tiles' % len(nexus_tiles)
        return nexus_tiles

    @staticmethod
    def _prune_tiles(nexus_tiles):
        del_ind = np.where([np.all(tile.data.mask) for tile in nexus_tiles])[0]
        for i in np.flipud(del_ind):
            del nexus_tiles[i]

    def _lat2ind(self,lat):
        return int((lat-self._minLatCent)/self._latRes)

    def _lon2ind(self,lon):
        return int((lon-self._minLonCent)/self._lonRes)

    def _create_nc_file_time1d(self, a, fname, varname, varunits=None,
                               fill=None):
        print 'a=',a
        print 'shape a = ', a.shape
        sys.stdout.flush()
        assert len(a.shape) == 1
        time_dim = len(a)
        rootgrp = Dataset(fname, "w", format="NETCDF4")
        rootgrp.createDimension("time", time_dim)
        vals = rootgrp.createVariable(varname, "f4", dimensions=("time",),
                                      fill_value=fill)
        times = rootgrp.createVariable("time", "f4", dimensions=("time",))
        vals[:] = [d['mean'] for d in a]
        times[:] = [d['time'] for d in a]
        if varunits is not None:
            vals.units = varunits
        times.units = 'seconds since 1970-01-01 00:00:00'
        rootgrp.close()

    def _create_nc_file_latlon2d(self, a, fname, varname, varunits=None,
                                 fill=None):
        print 'a=',a
        print 'shape a = ', a.shape
        sys.stdout.flush()
        assert len(a.shape) == 2
        lat_dim, lon_dim = a.shape
        rootgrp = Dataset(fname, "w", format="NETCDF4")
        rootgrp.createDimension("lat", lat_dim)
        rootgrp.createDimension("lon", lon_dim)
        vals = rootgrp.createVariable(varname, "f4",
                                      dimensions=("lat","lon",),
                                      fill_value=fill)
        lats = rootgrp.createVariable("lat", "f4", dimensions=("lat",))
        lons = rootgrp.createVariable("lon", "f4", dimensions=("lon",))
        vals[:,:] = a
        lats[:] = np.linspace(self._minLatCent, 
                              self._maxLatCent, lat_dim)
        lons[:] = np.linspace(self._minLonCent,
                              self._maxLonCent, lon_dim)
        if varunits is not None:
            vals.units = varunits
        lats.units = "degrees north"
        lons.units = "degrees east"
        rootgrp.close()

    def _create_nc_file(self, a, fname, varname, **kwargs):
        self._create_nc_file_latlon2d(a, fname, varname, **kwargs)
