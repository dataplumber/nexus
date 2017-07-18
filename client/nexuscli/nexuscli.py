"""
Copyright (c) 2017 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved

This module provides a native python client interface to the NEXUS (https://github.com/dataplumber/nexus) 
webservice API.

Usage:

    
    ``import nexuscli``
    
    ``nexuscli.set_target("http://nexus-webapp:8083")``
    ``nexuscli.dataset_list()``
    
"""
import requests
import numpy as np
from datetime import datetime
from collections import namedtuple, OrderedDict
from pytz import UTC

TimeSeries = namedtuple('TimeSeries', ('dataset', 'time', 'mean', 'standard_deviation', 'count', 'minimum', 'maximum'))
TimeSeries.__doc__ = '''\
A Time Series

dataset - Name of the Dataset
time - numpy array containing times
mean - numpy array containing means
standard_deviation - numpy array containing standard deviations
count - numpy array containing counts
minimum - numpy array containing minimums
maximum - numpy array containing maximums'''

ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

target = 'http://localhost:8083'

session = requests.session()


def set_target(url):
    """
    Set the URL for the NEXUS webapp endpoint.
    
    :param url: URL for NEXUS webapp endpoint 
    :return: None
    """
    global target
    target = url


def dataset_list():
    """
    Get a list of datasets and the start and end time for each.
    
    :return: list of datasets
    :rtype: list
    """
    response = session.get("{}/list".format(target))
    data = response.json()

    list_response = []
    for dataset in data:
        dataset['start'] = datetime.utcfromtimestamp(dataset['start'] / 1000).strftime(ISO_FORMAT)
        dataset['end'] = datetime.utcfromtimestamp(dataset['end'] / 1000).strftime(ISO_FORMAT)

        ordered_dict = OrderedDict()
        ordered_dict['shortName'] = dataset['shortName']
        ordered_dict['start'] = dataset['start']
        ordered_dict['end'] = dataset['end']
        list_response.append(ordered_dict)

    return list_response


def daily_difference_average(dataset, bounding_box, start_datetime, end_datetime):
    """
    Generate an anomaly Time series for a given dataset, bounding box, and timeframe.
    
    :param dataset: Name of the dataset
    :type dataset: str
    :param bounding_box: Bounding box for area of interest
    :type bounding_box: shapely.geometry.polygon.Polygon
    :param start_datetime: Start time
    :type start_datetime: datetime
    :param end_datetime: End time
    :type end_datetime: datetime
    
    :return: List of nexuscli.TimeSeries namedtuples
    :rtype: list
    """
    url = "{}/dailydifferenceaverage_spark?".format(target)

    params = {
        'dataset': dataset,
        'climatology': "{}_CLIM".format(dataset),
        'b': ','.join(str(b) for b in bounding_box.bounds),
        'startTime': start_datetime.strftime(ISO_FORMAT),
        'endTime': end_datetime.strftime(ISO_FORMAT),
    }

    response = session.get(url, params=params)
    response.raise_for_status()
    response = response.json()

    data = np.array(response['data']).flatten()

    assert len(data) > 0, "No data found in {} between {} and {} for Datasets {}.".format(bounding_box.wkt,
                                                                                          start_datetime.strftime(
                                                                                              ISO_FORMAT),
                                                                                          end_datetime.strftime(
                                                                                              ISO_FORMAT),
                                                                                          dataset)

    time_series_result = []

    key_to_index = {k: x for x, k in enumerate(data[0].keys())}

    time_series_data = np.array([tuple(each.values()) for each in [entry for entry in data]])

    if len(time_series_data) > 0:
        time_series_result.append(
            TimeSeries(
                dataset=dataset,
                time=np.array([datetime.utcfromtimestamp(t).replace(tzinfo=UTC) for t in
                               time_series_data[:, key_to_index['time']]]),
                mean=time_series_data[:, key_to_index['mean']],
                standard_deviation=time_series_data[:, key_to_index['std']],
                count=None,
                minimum=None,
                maximum=None,
            )
        )

    return time_series_result


def time_series(datasets, bounding_box, start_datetime, end_datetime, seasonal_filter=False, lowpass_filter=False,
                spark=False):
    """
    Send a request to NEXUS to calculate a time series.
    
    :param datasets: Sequence (max length 2) of the name of the dataset(s)
    :type datasets: iterable
    :param bounding_box: Bounding box for area of interest
    :type bounding_box: shapely.geometry.polygon.Polygon
    :param start_datetime: Start time
    :type start_datetime: datetime
    :param end_datetime: End time
    :type end_datetime: datetime
    :param seasonal_filter: Optionally calculate seasonal values to produce de-seasoned results. Default: false
    :type seasonal_filter: bool
    :param lowpass_filter: Optionally apply a lowpass filter. Default: false
    :type lowpass_filter: bool
    :param spark: Optionally use spark. Default: false
    :type spark: bool
    
    :return: List of nexuscli.TimeSeries namedtuples
    :rtype: list
    """

    assert 0 < len(datasets) <= 2, "datasets must be a sequence of 1 or 2 items"
    if spark:
        url = "{}/timeSeriesSpark?".format(target)
    else:
        url = "{}/stats?".format(target)

    params = {
        'ds': ','.join(datasets),
        'b': ','.join(str(b) for b in bounding_box.bounds),
        'startTime': start_datetime.strftime(ISO_FORMAT),
        'endTime': end_datetime.strftime(ISO_FORMAT),
        'seasonalFilter': seasonal_filter,
        'lowPassFilter': lowpass_filter
    }

    response = session.get(url, params=params)
    response.raise_for_status()
    response = response.json()

    data = np.array(response['data']).flatten()

    assert len(data) > 0, "No data found in {} between {} and {} for Datasets {}.".format(bounding_box.wkt,
                                                                                          start_datetime.strftime(
                                                                                              ISO_FORMAT),
                                                                                          end_datetime.strftime(
                                                                                              ISO_FORMAT),
                                                                                          datasets)

    time_series_result = []

    for i in range(0, len(response['meta'])):
        key_to_index = {k: x for x, k in enumerate(data[0].keys())}

        time_series_data = np.array([tuple(each.values()) for each in [entry for entry in data if entry['ds'] == i]])

        if len(time_series_data) > 0:
            time_series_result.append(
                TimeSeries(
                    dataset=response['meta'][i]['shortName'],
                    time=np.array([datetime.utcfromtimestamp(t).replace(tzinfo=UTC) for t in
                                   time_series_data[:, key_to_index['time']]]),
                    mean=time_series_data[:, key_to_index['mean']],
                    standard_deviation=time_series_data[:, key_to_index['std']],
                    count=time_series_data[:, key_to_index['cnt']],
                    minimum=time_series_data[:, key_to_index['min']],
                    maximum=time_series_data[:, key_to_index['max']],
                )
            )

    return time_series_result
