"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import sys
import os
import math
import numpy as np
from time import time
from webservice.SparkAlg import SparkAlg
from webservice.NexusHandler import NexusHandler, nexus_handler, DEFAULT_PARAMETERS_SPEC
from nexustiles.nexustiles import NexusTileService
from webservice.webmodel import NexusProcessingException
from pyspark import SparkContext,SparkConf

# @nexus_handler
class CorrMapSparkHandlerImpl(SparkAlg):
    name = "Correlation Map Spark"
    path = "/corrMapSpark"
    description = "Computes a correlation map between two datasets given an arbitrary geographical area and time range"
    params = DEFAULT_PARAMETERS_SPEC
    singleton = True

    def __init__(self):
        SparkAlg.__init__(self)

    @staticmethod
    def _map(tile_in):
        # Unpack input
        tile_bounds, start_time, end_time, ds = tile_in
        (min_lat, max_lat, min_lon, max_lon, 
         min_y, max_y, min_x, max_x) = tile_bounds

        # Create arrays to hold intermediate results during
        # correlation coefficient calculation.
        tile_inbounds_shape = (max_y-min_y+1, max_x-min_x+1)
        sumx_tile = np.zeros(tile_inbounds_shape, dtype=np.float64)
        sumy_tile = np.zeros(tile_inbounds_shape, dtype=np.float64)
        sumxx_tile = np.zeros(tile_inbounds_shape, dtype=np.float64)
        sumyy_tile = np.zeros(tile_inbounds_shape, dtype=np.float64)
        sumxy_tile = np.zeros(tile_inbounds_shape, dtype=np.float64)
        n_tile = np.zeros(tile_inbounds_shape, dtype=np.uint32)

        # Can only retrieve some number of days worth of data from Solr
        # at a time.  Set desired value here.
        days_at_a_time = 90
        #days_at_a_time = 30
        #days_at_a_time = 7
        #days_at_a_time = 1
        print 'days_at_a_time = ', days_at_a_time
        t_incr = 86400 * days_at_a_time

        tile_service = NexusTileService()

        # Compute the intermediate summations needed for the Pearson 
        # Correlation Coefficient.  We use a one-pass online algorithm
        # so that not all of the data needs to be kept in memory all at once.
        t_start = start_time
        while t_start <= end_time:
            t_end = min(t_start+t_incr,end_time)
            t1 = time()
            print 'nexus call start at time %f' % t1
            sys.stdout.flush()
            ds1tiles = tile_service.get_tiles_bounded_by_box(min_lat, 
                                                             max_lat, 
                                                             min_lon, 
                                                             max_lon, 
                                                             ds[0], 
                                                             t_start,
                                                             t_end)
            ds2tiles = tile_service.get_tiles_bounded_by_box(min_lat, 
                                                             max_lat, 
                                                             min_lon, 
                                                             max_lon, 
                                                             ds[1], 
                                                             t_start,
                                                             t_end)
            t2 = time()
            print 'nexus call end at time %f' % t2
            print 'secs in nexus call: ', t2-t1
            sys.stdout.flush()
            
            len1 = len(ds1tiles)
            len2 = len(ds2tiles)
            print 't %d to %d - Got %d and %d tiles' % (t_start, t_end, 
                                                        len1, len2)
            sys.stdout.flush()
            i1 = 0
            i2 = 0
            time1 = 0
            time2 = 0
            while i1 < len1 and i2 < len2:
                tile1 = ds1tiles[i1]
                tile2 = ds2tiles[i2]
                #print 'tile1.data = ',tile1.data
                #print 'tile2.data = ',tile2.data
                #print 'i1, i2, t1, t2 times: ', i1, i2, tile1.times[0], tile2.times[0]
                assert tile1.times[0] >= time1, 'DS1 time out of order!'
                assert tile2.times[0] >= time2, 'DS2 time out of order!'
                time1 = tile1.times[0]
                time2 = tile2.times[0]
                #print 'i1=%d,i2=%d,time1=%d,time2=%d'%(i1,i2,time1,time2)
                if time1 < time2: 
                    i1 += 1
                    continue
                elif time2 < time1:
                    i2 += 1
                    continue
                assert (time1 == time2),\
                    "Mismatched tile times %d and %d" % (time1, time2)
                #print 'processing time:',time1,time2
                t1_data = tile1.data.data
                t1_mask = tile1.data.mask
                t2_data = tile2.data.data
                t2_mask = tile2.data.mask
                t1_data = np.nan_to_num(t1_data)
                t2_data = np.nan_to_num(t2_data)
                joint_mask = ((~t1_mask).astype(np.uint8) *
                              (~t2_mask).astype(np.uint8))
                #print 'joint_mask=',joint_mask
                sumx_tile += (t1_data[0,min_y:max_y+1,min_x:max_x+1] *
                              joint_mask[0,min_y:max_y+1,min_x:max_x+1])
                #print 'sumx_tile=',sumx_tile
                sumy_tile += (t2_data[0,min_y:max_y+1,min_x:max_x+1] *
                              joint_mask[0,min_y:max_y+1,min_x:max_x+1])
                #print 'sumy_tile=',sumy_tile
                sumxx_tile += (t1_data[0,min_y:max_y+1,min_x:max_x+1] *
                               t1_data[0,min_y:max_y+1,min_x:max_x+1] *
                               joint_mask[0,min_y:max_y+1,min_x:max_x+1])
                #print 'sumxx_tile=',sumxx_tile
                sumyy_tile += (t2_data[0,min_y:max_y+1,min_x:max_x+1] *
                               t2_data[0,min_y:max_y+1,min_x:max_x+1] *
                               joint_mask[0,min_y:max_y+1,min_x:max_x+1])
                #print 'sumyy_tile=',sumyy_tile
                sumxy_tile += (t1_data[0,min_y:max_y+1,min_x:max_x+1] *
                               t2_data[0,min_y:max_y+1,min_x:max_x+1] *
                               joint_mask[0,min_y:max_y+1,min_x:max_x+1])
                #print 'sumxy_tile=',sumxy_tile
                n_tile += joint_mask[0,min_y:max_y+1,min_x:max_x+1]
                #print 'n_tile=',n_tile
                i1 += 1
                i2 += 1
            t_start = t_end + 1

        print 'Finished tile', tile_bounds
        sys.stdout.flush()
        return ((min_lat,max_lat,min_lon,max_lon),(sumx_tile,sumy_tile,
                                                   sumxx_tile,sumyy_tile,
                                                   sumxy_tile, n_tile))

    def calc(self, computeOptions, **args):

        spark_master,spark_nexecs,spark_nparts = computeOptions.get_spark_cfg()
        self._setQueryParams(computeOptions.get_dataset(),
                             (float(computeOptions.get_min_lat()),
                              float(computeOptions.get_max_lat()),
                              float(computeOptions.get_min_lon()),
                              float(computeOptions.get_max_lon())),
                             computeOptions.get_start_time(),
                             computeOptions.get_end_time(),
                             spark_master=spark_master,
                             spark_nexecs=spark_nexecs,
                             spark_nparts=spark_nparts)

        print 'ds = ',self._ds
        if not len(self._ds) == 2:
            raise Exception("Requires two datasets for comparison. Specify request parameter ds=Dataset_1,Dataset_2")

        self._find_native_resolution()
        print 'Using Native resolution: lat_res=%f, lon_res=%f' % (self._latRes, self._lonRes)
        self._minLatCent = self._minLat + self._latRes / 2
        self._minLonCent = self._minLon + self._lonRes / 2
        nlats = int((self._maxLat-self._minLatCent)/self._latRes)+1
        nlons = int((self._maxLon-self._minLonCent)/self._lonRes)+1
        self._maxLatCent = self._minLatCent + (nlats-1) * self._latRes
        self._maxLonCent = self._minLonCent + (nlons-1) * self._lonRes
        print 'nlats=',nlats,'nlons=',nlons
        sys.stdout.flush()

        nexus_tiles = self._find_global_tile_set()
        # print 'tiles:'
        # for tile in nexus_tiles:
        #     print tile.granule
        #     print tile.section_spec
        #     print 'lat:', tile.latitudes
        #     print 'lon:', tile.longitudes

        #                                                          nexus_tiles)
        if len(nexus_tiles) == 0:
            raise NexusProcessingException.NoDataException(reason="No data found for selected timeframe")

        print 'Found %d tiles' % len(nexus_tiles)
        sys.stdout.flush()
        # Create array of tuples to pass to Spark map function
        nexus_tiles_spark = [[self._find_tile_bounds(t), 
                              self._startTime, self._endTime, 
                              self._ds] for t in nexus_tiles]

        # Remove empty tiles (should have bounds set to None)
        bad_tile_inds = np.where([t[0] is None for t in nexus_tiles_spark])[0]
        for i in np.flipud(bad_tile_inds):
            del nexus_tiles_spark[i]

        # Expand Spark map tuple array by duplicating each entry N times,
        # where N is the number of ways we want the time dimension carved up.
        num_time_parts = 72
        #num_time_parts = 2
        #num_time_parts = 1
        nexus_tiles_spark = np.repeat(nexus_tiles_spark, num_time_parts, axis=0)
        print 'repeated len(nexus_tiles_spark) = ', len(nexus_tiles_spark)
        
        # Set the time boundaries for each of the Spark map tuples.
        # Every Nth element in the array gets the same time bounds.
        spark_part_times = np.linspace(self._startTime, self._endTime+1,
                                       num_time_parts+1, dtype=np.int64)
        
        spark_part_time_ranges = \
            np.repeat([[[spark_part_times[i], 
                         spark_part_times[i+1]-1] for i in range(num_time_parts)]],
                      len(nexus_tiles_spark) / num_time_parts, axis=0).reshape((len(nexus_tiles_spark), 2))
        #print 'spark_part_time_ranges=', spark_part_time_ranges
        nexus_tiles_spark[:,1:3] = spark_part_time_ranges
        #print 'nexus_tiles_spark final = '
        for i in range(len(nexus_tiles_spark)):
            print nexus_tiles_spark[i]

        # Configure Spark
        sp_conf = SparkConf()
        sp_conf.setAppName("Spark Correlation Map")
        sp_conf.set("spark.executorEnv.HOME",
                    os.path.join(os.getenv('HOME'), 'spark_exec_home'))
        sp_conf.set("spark.executorEnv.PYTHONPATH", os.getcwd())
        sp_conf.set("spark.executor.memoryOverhead", "4g")

        cores_per_exec = 1
        if spark_master == "mesos":
            # For Mesos, the master is set from environment variable MASTER
            # and number of executors is set from spark.cores.max.
            sp_conf.set("spark.cores.max", spark_nexecs)
        else:
            # Master is "yarn" or "local[N]" (not Mesos)
            sp_conf.setMaster(spark_master)
            sp_conf.set("spark.executor.instances", spark_nexecs)
        sp_conf.set("spark.executor.cores", cores_per_exec)

        #print sp_conf.getAll()
        sc = SparkContext(conf=sp_conf)
        
        # Launch Spark computations
        #print 'nexus_tiles_spark=',nexus_tiles_spark
        rdd = sc.parallelize(nexus_tiles_spark,self._spark_nparts)
        sum_tiles_part = rdd.map(self._map)
        #print "sum_tiles_part = ",sum_tiles_part.collect()
        sum_tiles = \
            sum_tiles_part.combineByKey(lambda val: val,
                                        lambda x,val: (x[0]+val[0], 
                                                       x[1]+val[1],
                                                       x[2]+val[2],
                                                       x[3]+val[3],
                                                       x[4]+val[4],
                                                       x[5]+val[5]),
                                        lambda x,y: (x[0]+y[0], 
                                                     x[1]+y[1],
                                                     x[2]+y[2],
                                                     x[3]+y[3],
                                                     x[4]+y[4],
                                                     x[5]+y[5]))
        # Convert the N (pixel-wise count) array for each tile to be a 
        # NumPy masked array.  That is the last array in the tuple of 
        # intermediate summation arrays.  Set mask to True if count is 0.
        sum_tiles = \
            sum_tiles.map(lambda (bounds, (sum_x, sum_y, sum_xx, 
                                           sum_yy, sum_xy, n)):
                              (bounds, (sum_x, sum_y, sum_xx, sum_yy, sum_xy,
                                        np.ma.array(n, 
                                                    mask=~(n.astype(bool))))))

        #print 'sum_tiles = ',sum_tiles.collect()

        # For each pixel in each tile compute an array of Pearson 
        # correlation coefficients.  The map function is called once 
        # per tile.  The result of this map operation is a list of 3-tuples of
        # (bounds, r, n) for each tile (r=Pearson correlation coefficient
        # and n=number of input values that went into each pixel with 
        # any masked values not included).
        corr_tiles = \
            sum_tiles.map(lambda (bounds, (sum_x, sum_y, sum_xx, sum_yy, 
                                           sum_xy, n)):
                              (bounds, 
                               np.ma.array(((sum_xy-sum_x*sum_y/n) /
                                np.sqrt((sum_xx-sum_x*sum_x/n)*
                                        (sum_yy-sum_y*sum_y/n))),
                                           mask=~(n.astype(bool))),
                               n)).collect()
            
        r = np.zeros((nlats, nlons),dtype=np.float64,order='C')

        # The tiles below are NOT Nexus objects.  They are tuples
        # with the following for each correlation map subset:
        # (1) lat-lon bounding box, (2) array of correlation r values, 
        # and (3) array of count n values.
        for tile in corr_tiles:
            ((tile_min_lat, tile_max_lat, tile_min_lon, tile_max_lon),
             tile_data, tile_cnt) = tile
            y0 = self._lat2ind(tile_min_lat)
            y1 = self._lat2ind(tile_max_lat)
            x0 = self._lon2ind(tile_min_lon)
            x1 = self._lon2ind(tile_max_lon)
            print 'writing tile lat %f-%f, lon %f-%f, map y %d-%d, map x %d-%d' % \
                (tile_min_lat, tile_max_lat, 
                 tile_min_lon, tile_max_lon, y0, y1, x0, x1)
            sys.stdout.flush()
            r[y0:y1+1,x0:x1+1] = tile_data

        # Store global map in a NetCDF file.
        self._create_nc_file(r, 'corrmap.nc', 'r')

        # Stop the SparkContext.
        sc.stop()

        return [[]], None, None
