"""
Copyright (c) 2017 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import requests
import numpy as np
from datetime import datetime
from collections import namedtuple
from pytz import UTC

TimeSeries = namedtuple('TimeSeries', ('dataset', 'time', 'mean', 'standard_deviation', 'count', 'minimum', 'maximum'))

ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

target = 'http://localhost:8083'

session = requests.session()


def set_target(url):
    global target
    target = url


def daily_difference_average(dataset, bounding_box, start_datetime, end_datetime, spark=False):
    if spark:
        url = "{}/dailydifferenceaverage_spark?".format(target)
    else:
        url = "{}/dailydifferenceaverage?".format(target)

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


def time_series(datasets, bounding_box, start_datetime, end_datetime, seasonal_filter=False, lowpass_filter=False,
                spark=False):
    """
    Send a request to NEXUS to calculate a time series.
    
    :param datasets: Sequence (max length 2) of the name of the dataset(s) 
    :param bounding_box: Bounding box for area of interest
    :param start_datetime: Start time
    :param end_datetime: End time
    :param seasonal_filter: Optionally calculate seasonal values to produce de-seasoned results
    :param lowpass_filter: Optionally apply a lowpass filter
    :param spark: Optionally use spark
    :return: List of nexuscli.TimeSeries namedtuples.
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

    data = np.array(response['data']).T

    time_series_result = []

    for i in range(0, len(response['meta'])):
        key_to_index = {k: x for x, k in enumerate(data[i][0].keys())}
        time_series_data = np.array([tuple(each.values()) for each in data[i]])

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
