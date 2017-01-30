"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

import ConfigParser
import logging
import uuid
from datetime import datetime

import pkg_resources
from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy
from cassandra.query import BatchStatement
from pytz import UTC


class AbstractResultsContainer:
    def __init__(self):
        self._log = logging.getLogger(__name__)
        self._log.info("Creating DOMS Results Storage Instance")

        self._session = None

    def __enter__(self):
        domsconfig = ConfigParser.RawConfigParser()
        domsconfig.readfp(pkg_resources.resource_stream(__name__, "domsconfig.ini"), filename='domsconfig.ini')

        cassHost = domsconfig.get("cassandra", "host")
        cassKeyspace = domsconfig.get("cassandra", "keyspace")
        cassDatacenter = domsconfig.get("cassandra", "local_datacenter")
        cassVersion = int(domsconfig.get("cassandra", "protocol_version"))

        dc_policy = DCAwareRoundRobinPolicy(cassDatacenter)
        token_policy = TokenAwarePolicy(dc_policy)

        self._cluster = Cluster([host for host in cassHost.split(',')], load_balancing_policy=token_policy,
                                protocol_version=cassVersion)

        self._session = self._cluster.connect(cassKeyspace)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cluster.shutdown()

    def _parseDatetime(self, dtString):
        dt = datetime.strptime(dtString, "%Y-%m-%dT%H:%M:%SZ")
        epoch = datetime.utcfromtimestamp(0)
        time = (dt - epoch).total_seconds() * 1000.0
        return int(time)


class ResultsStorage(AbstractResultsContainer):
    def __init__(self):
        AbstractResultsContainer.__init__(self)

    def insertResults(self, results, params, stats, startTime, completeTime, userEmail, execution_id=None):
        if type(execution_id) is str:
            execution_id = uuid.UUID(execution_id)

        execution_id = self.insertExecution(execution_id, startTime, completeTime, userEmail)
        self.__insertParams(execution_id, params)
        self.__insertStats(execution_id, stats)
        self.__insertResults(execution_id, results)
        return execution_id

    def insertExecution(self, execution_id, startTime, completeTime, userEmail):
        if execution_id is None:
            execution_id = uuid.uuid4()

        cql = "INSERT INTO doms_executions (id, time_started, time_completed, user_email) VALUES (%s, %s, %s, %s)"
        self._session.execute(cql, (execution_id, startTime, completeTime, userEmail))
        return execution_id

    def __insertParams(self, execution_id, params):
        cql = """INSERT INTO doms_params
                    (execution_id, primary_dataset, matchup_datasets, depth_min, depth_max, time_tolerance, radius_tolerance, start_time, end_time, platforms, bounding_box, parameter)
                 VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        self._session.execute(cql, (execution_id,
                                    params["primary"],
                                    ",".join(params["matchup"]) if type(params["matchup"]) == list else params[
                                        "matchup"],
                                    params["depthMin"] if "depthMin" in params.keys() else None,
                                    params["depthMax"] if "depthMax" in params.keys() else None,
                                    int(params["timeTolerance"]),
                                    params["radiusTolerance"],
                                    params["startTime"],
                                    params["endTime"],
                                    params["platforms"],
                                    params["bbox"],
                                    params["parameter"]
                                    ))

    def __insertStats(self, execution_id, stats):
        cql = """
           INSERT INTO doms_execution_stats
                (execution_id, num_gridded_matched, num_gridded_checked, num_insitu_matched, num_insitu_checked, time_to_complete)
           VALUES
                (%s, %s, %s, %s, %s, %s)
        """
        self._session.execute(cql, (
            execution_id,
            stats["numGriddedMatched"],
            stats["numGriddedChecked"],
            stats["numInSituMatched"],
            stats["numInSituRecords"],
            stats["timeToComplete"]
        ))

    def __insertResults(self, execution_id, results):

        cql = """
           INSERT INTO doms_data
                (id, execution_id, value_id, primary_value_id, x, y, source_dataset, measurement_time, platform, device, measurement_values, is_primary)
           VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        insertStatement = self._session.prepare(cql)
        batch = BatchStatement()

        for result in results:
            self.__insertResult(execution_id, None, result, batch, insertStatement)

        self._session.execute(batch)

    def __insertResult(self, execution_id, primaryId, result, batch, insertStatement):

        dataMap = self.__buildDataMap(result)
        result_id = uuid.uuid4()
        batch.add(insertStatement, (
            result_id,
            execution_id,
            result["id"],
            primaryId,
            result["x"],
            result["y"],
            result["source"],
            result["time"],
            result["platform"] if "platform" in result else None,
            result["device"] if "device" in result else None,
            dataMap,
            1 if primaryId is None else 0
        )
                  )

        n = 0
        if "matches" in result:
            for match in result["matches"]:
                self.__insertResult(execution_id, result["id"], match, batch, insertStatement)
                n += 1
                if n >= 20:
                    if primaryId is None:
                        self.__commitBatch(batch)
                    n = 0

        if primaryId is None:
            self.__commitBatch(batch)

    def __commitBatch(self, batch):
        self._session.execute(batch)
        batch.clear()

    def __buildDataMap(self, result):
        dataMap = {}
        for name in result:
            value = result[name]
            if name not in ["id", "x", "y", "source", "time", "platform", "device", "point", "matches"] and type(
                    value) in [float, int]:
                dataMap[name] = value
        return dataMap


class ResultsRetrieval(AbstractResultsContainer):
    def __init__(self):
        AbstractResultsContainer.__init__(self)

    def retrieveResults(self, id, trim_data=False):
        params = self.__retrieveParams(id)
        stats = self.__retrieveStats(id)
        data = self.__retrieveData(id, trim_data=trim_data)
        return params, stats, data

    def __retrieveData(self, id, trim_data=False):
        dataMap = self.__retrievePrimaryData(id, trim_data=trim_data)
        self.__enrichPrimaryDataWithMatches(id, dataMap, trim_data=trim_data)
        data = [dataMap[name] for name in dataMap]
        return data

    def __enrichPrimaryDataWithMatches(self, id, dataMap, trim_data=False):
        cql = "SELECT * FROM doms_data where execution_id = %s and is_primary = false"
        rows = self._session.execute(cql, (id,))

        for row in rows:
            entry = self.__rowToDataEntry(row, trim_data=trim_data)
            if row.primary_value_id in dataMap:
                if not "matches" in dataMap[row.primary_value_id]:
                    dataMap[row.primary_value_id]["matches"] = []
                dataMap[row.primary_value_id]["matches"].append(entry)
            else:
                print row

    def __retrievePrimaryData(self, id, trim_data=False):
        cql = "SELECT * FROM doms_data where execution_id = %s and is_primary = true"
        rows = self._session.execute(cql, (id,))

        dataMap = {}
        for row in rows:
            entry = self.__rowToDataEntry(row, trim_data=trim_data)
            dataMap[row.value_id] = entry
        return dataMap

    def __rowToDataEntry(self, row, trim_data=False):
        if trim_data:
            entry = {
                "x": str(float(row.x)),
                "y": str(float(row.y)),
                "source": row.source_dataset,
                "time": row.measurement_time.replace(tzinfo=UTC)
            }
        else:
            entry = {
                "id": row.value_id,
                "x": str(float(row.x)),
                "y": str(float(row.y)),
                "source": row.source_dataset,
                "device": row.device,
                "platform": row.platform,
                "time": row.measurement_time.replace(tzinfo=UTC)
            }
        for key in row.measurement_values:
            value = str(float(row.measurement_values[key]))
            entry[key] = value
        return entry

    def __retrieveStats(self, id):
        cql = "SELECT * FROM doms_execution_stats where execution_id = %s limit 1"
        rows = self._session.execute(cql, (id,))
        for row in rows:
            stats = {
                "numGriddedMatched": row.num_gridded_matched,
                "numGriddedChecked": row.num_gridded_checked,
                "numInSituMatched": row.num_insitu_matched,
                "numInSituChecked": row.num_insitu_checked,
                "timeToComplete": row.time_to_complete
            }
            return stats

        raise Exception("Execution not found with id '%s'" % id)

    def __retrieveParams(self, id):
        cql = "SELECT * FROM doms_params where execution_id = %s limit 1"
        rows = self._session.execute(cql, (id,))
        for row in rows:
            params = {
                "primary": row.primary_dataset,
                "matchup": row.matchup_datasets.split(","),
                "depthMin": str(float(row.depth_min)),
                "depthMax": str(float(row.depth_max)),
                "timeTolerance": row.time_tolerance,
                "radiusTolerance": str(float(row.radius_tolerance)),
                "startTime": row.start_time.replace(tzinfo=UTC),
                "endTime": row.end_time.replace(tzinfo=UTC),
                "platforms": row.platforms,
                "bbox": row.bounding_box,
                "parameter": row.parameter
            }
            return params

        raise Exception("Execution not found with id '%s'" % id)
