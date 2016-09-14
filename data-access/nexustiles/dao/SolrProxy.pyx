import logging
import time
from datetime import datetime

import solr


class SolrProxy(object):
    def __init__(self, config):
        self.config = config
        self.solrUrl = config.get("solr", "host")
        self.solrCore = config.get("solr", "core")
        self.logger = logging.getLogger('nexus')
        self.solrcon = solr.Solr('http://%s/solr/%s' % (self.solrUrl, self.solrCore))

    def find_tile_by_id(self, tile_id):

        search = 'id:%s' % tile_id

        params = {
            'rows': 1
        }

        results, start, found = self.do_query(*(search, None, None, True, None), **params)

        assert len(results) == 1, "Found %s results, expected exactly 1" % len(results)
        return [results[0]]

    def get_data_series_list(self):
        search = "*:*"
        params = {
            "facet": "true",
            "facet.field": "dataset_s",
            "facet.pivot": "{!stats=piv1}dataset_s",
            "stats": "on",
            "stats.field": "{!tag=piv1 min=true max=true sum=false}tile_max_time_dt"
        }

        response = self.do_query_raw(*(search, None, None, False, None), **params)

        l = []
        for g in response.facet_counts["facet_pivot"]["dataset_s"]:
            shortName = g["value"]
            startTime = time.mktime(g["stats"]["stats_fields"]["tile_max_time_dt"]["min"].timetuple()) * 1000
            endTime = time.mktime(g["stats"]["stats_fields"]["tile_max_time_dt"]["max"].timetuple()) * 1000
            l.append({
                "shortName": shortName,
                "title": shortName,
                "start": startTime,
                "end": endTime
            })
        l = sorted(l, key=lambda entry: entry["title"])
        return l

    def find_tile_by_bbox_and_most_recent_day_of_year(self, min_lat, max_lat, min_lon, max_lon, ds, day_of_year):

        search = 'dataset_s:%s' % ds

        params = {
            'fq': [
                "geo:[%s,%s TO %s,%s]" % (min_lat, min_lon, max_lat, max_lon),
                "tile_count_i:[1 TO *]",
                "day_of_year_i:[* TO %s]" % day_of_year
            ],
            'rows': 1
        }

        results, start, found = self.do_query(
            *(search, None, None, True, ('day_of_year_i desc',)), **params)

        return [results[0]]

    def find_days_in_range_asc(self, min_lat, max_lat, min_lon, max_lon, ds, start_time, end_time, **kwargs):

        search = 'dataset_s:%s' % ds

        search_start_s = datetime.utcfromtimestamp(start_time).strftime('%Y-%m-%dT%H:%M:%SZ')
        search_end_s = datetime.utcfromtimestamp(end_time).strftime('%Y-%m-%dT%H:%M:%SZ')

        additionalparams = {
            'fq': [
                "geo:[%s,%s TO %s,%s]" % (min_lat, min_lon, max_lat, max_lon),
                "{!frange l=0 u=0}ms(tile_min_time_dt,tile_max_time_dt)",
                "tile_count_i:[1 TO *]",
                "tile_min_time_dt:[%s TO %s] " % (search_start_s, search_end_s)
            ],
            'rows': 0,
            'facet': 'true',
            'facet_field': 'tile_min_time_dt',
            'facet_mincount': '1',
            'facet_limit': '-1'
        }

        self._merge_kwargs(additionalparams, **kwargs)

        response = self.do_query_raw(*(search, None, None, False, None), **additionalparams)

        daysinrangeasc = sorted(
            [(datetime.strptime(a_date, '%Y-%m-%dT%H:%M:%SZ') - datetime.utcfromtimestamp(0)).total_seconds() for a_date
             in response.facet_counts['facet_fields']['tile_min_time_dt'].keys()])

        return daysinrangeasc

    def find_all_tiles_in_box_sorttimeasc(self, min_lat, max_lat, min_lon, max_lon, ds, start_time=0,
                                          end_time=-1, **kwargs):

        search = 'dataset_s:%s' % ds

        additionalparams = {
            'fq': [
                "geo:[%s,%s TO %s,%s]" % (min_lat, min_lon, max_lat, max_lon),
                "tile_count_i:[1 TO *]"
            ]
        }

        if 0 < start_time <= end_time:
            search_start_s = datetime.utcfromtimestamp(start_time).strftime('%Y-%m-%dT%H:%M:%SZ')
            search_end_s = datetime.utcfromtimestamp(end_time).strftime('%Y-%m-%dT%H:%M:%SZ')

            time_clause = "(" \
                          "tile_min_time_dt:[%s TO %s] " \
                          "OR tile_max_time_dt:[%s TO %s] " \
                          "OR (tile_min_time_dt:[* TO %s] AND tile_max_time_dt:[%s TO *])" \
                          ")" % (
                              search_start_s, search_end_s,
                              search_start_s, search_end_s,
                              search_start_s, search_end_s
                          )
            additionalparams['fq'].append(time_clause)

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(
            *(search, None, None, False, 'tile_min_time_dt asc, tile_max_time_dt asc'),
            **additionalparams)

    def find_all_tiles_in_polygon_sorttimeasc(self, bounding_polygon, ds, start_time=0, end_time=-1, **kwargs):

        print "Called find_all_tiles_in_polygon_sorttimeasc with params: %s, %s, %s, %s, %s" % (bounding_polygon, ds, start_time, end_time, kwargs)

        search = 'dataset_s:%s' % ds

        additionalparams = {
            'fq': [
                "{!field f=geo}Intersects(%s)" % bounding_polygon.wkt,
                "tile_count_i:[1 TO *]"
            ]
        }

        if 0 < start_time <= end_time:
            search_start_s = datetime.utcfromtimestamp(start_time).strftime('%Y-%m-%dT%H:%M:%SZ')
            search_end_s = datetime.utcfromtimestamp(end_time).strftime('%Y-%m-%dT%H:%M:%SZ')

            time_clause = "(" \
                          "tile_min_time_dt:[%s TO %s] " \
                          "OR tile_max_time_dt:[%s TO %s] " \
                          "OR (tile_min_time_dt:[* TO %s] AND tile_max_time_dt:[%s TO *])" \
                          ")" % (
                              search_start_s, search_end_s,
                              search_start_s, search_end_s,
                              search_start_s, search_end_s
                          )
            additionalparams['fq'].append(time_clause)

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(
            *(search, None, None, False, 'tile_min_time_dt asc, tile_max_time_dt asc'),
            **additionalparams)

    def find_all_tiles_in_box_at_time(self, min_lat, max_lat, min_lon, max_lon, ds, time, **kwargs):
        search = 'dataset_s:%s' % ds

        the_time = datetime.utcfromtimestamp(time).strftime('%Y-%m-%dT%H:%M:%SZ')
        time_clause = "(" \
                      "tile_min_time_dt:[* TO %s] " \
                      "AND tile_max_time_dt:[%s TO *] " \
                      ")" % (
                          the_time, the_time
                      )

        additionalparams = {
            'fq': [
                "geo:[%s,%s TO %s,%s]" % (min_lat, min_lon, max_lat, max_lon),
                "tile_count_i:[1 TO *]",
                time_clause
            ]
        }

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(*(search, None, None, False, None), **additionalparams)

    def find_all_tiles_within_box_at_time(self, min_lat, max_lat, min_lon, max_lon, ds, time, **kwargs):
        search = 'dataset_s:%s' % ds

        the_time = datetime.utcfromtimestamp(time).strftime('%Y-%m-%dT%H:%M:%SZ')
        time_clause = "(" \
                      "tile_min_time_dt:[* TO %s] " \
                      "AND tile_max_time_dt:[%s TO *] " \
                      ")" % (
                          the_time, the_time
                      )

        additionalparams = {
            'fq': [
                "geo:\"Within(ENVELOPE(%s,%s,%s,%s))\"" % (min_lon, max_lon, max_lat, min_lat),
                "tile_count_i:[1 TO *]",
                time_clause
            ]
        }

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(*(search, "product(tile_avg_val_d, tile_count_i),*", None, False, None),
                                 **additionalparams)

    def find_all_boundary_tiles_at_time(self, min_lat, max_lat, min_lon, max_lon, ds, time, **kwargs):
        search = 'dataset_s:%s' % ds

        the_time = datetime.utcfromtimestamp(time).strftime('%Y-%m-%dT%H:%M:%SZ')
        time_clause = "(" \
                      "tile_min_time_dt:[* TO %s] " \
                      "AND tile_max_time_dt:[%s TO *] " \
                      ")" % (
                          the_time, the_time
                      )

        additionalparams = {
            'fq': [
                "geo:\"Intersects(MultiLineString((%s %s, %s %s),(%s %s, %s %s),(%s %s, %s %s),(%s %s, %s %s)))\"" % (
                    min_lon, max_lat, max_lon, max_lat, min_lon, max_lat, min_lon, min_lat, max_lon, max_lat, max_lon,
                    min_lat, min_lon, min_lat, max_lon, min_lat),
                "-geo:\"Within(ENVELOPE(%s,%s,%s,%s))\"" % (min_lon, max_lon, max_lat, min_lat),
                "tile_count_i:[1 TO *]",
                time_clause
            ]
        }

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(*(search, None, None, False, None), **additionalparams)

    def do_query(self, *args, **params):

        response = self.do_query_raw(*args, **params)

        return response.results, response.start, response.numFound

    def do_query_raw(self, *args, **params):

        # fl only works when passed as the second argument to solrcon.select
        try:
            fl = params['fl']
            del(params['fl'])
        except KeyError:
            fl = None

        args = (args[0],) + (fl,) + (args[2:])

        response = self.solrcon.select(*args, **params)

        return response

    def do_query_all(self, *args, **params):

        # fl only works when passed as the second argument to solrcon.select
        try:
            fl = params['fl']
            del(params['fl'])
        except KeyError:
            fl = None

        args = (args[0],) + (fl,) + (args[2:])

        results = []
        response = self.solrcon.select(*args, **params)
        results.extend(response.results)

        while len(results) < response.numFound:
            params['start'] = len(results)
            response = self.solrcon.select(*args, **params)
            results.extend(response.results)

        assert len(results) == response.numFound

        return results

    @staticmethod
    def _merge_kwargs(additionalparams, **kwargs):
        # Only Solr-specific kwargs are parsed
        try:
            additionalparams['rows'] = kwargs['rows']
        except KeyError:
            pass

        try:
            additionalparams['start'] = kwargs['start']
        except KeyError:
            pass

        try:
            kwfq = kwargs['fq'] if isinstance(kwargs['fq'], list) else list(kwargs['fq'])
        except KeyError:
            kwfq = []

        try:
            additionalparams['fq'].extend(kwfq)
        except KeyError:
            additionalparams['fq'] = kwfq

        try:
            kwfl = kwargs['fl'] if isinstance(kwargs['fl'], list) else [kwargs['fl']]
        except KeyError:
            kwfl = []

        try:
            additionalparams['fl'].extend(kwfl)
        except KeyError:
            additionalparams['fl'] = kwfl
