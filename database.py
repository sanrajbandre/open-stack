"""
Database Access Layer
=====================

This module encapsulates persistence for the AI agent.  A MySQL
database is used to store job metadata, execution metrics and
recommendations.  By centralising database access in one module, it
becomes easier to swap out the database backend (e.g. PostgreSQL or
SQLite) if needed.

The default schema comprises three tables:

* ``jobs`` – stores basic metadata returned from YARN about each
  application.  Columns include application ID, user, name, queue,
  state, start and finish times and resource usage statistics (e.g.
  memorySeconds and vcoreSeconds【762643672411904†L4727-L4851】).
* ``spark_metrics`` – stores granular executor metrics per job as
  returned by the Spark history server.  Each row stores one metric
  value for a given application and executor (e.g. peak memory,
  executorRunTime)【383517666583736†L417-L423】.
* ``recommendations`` – stores the category (hourly, daily, ad‑hoc),
  analysis results and resource tuning recommendations for each job.

The ``Database`` class is responsible for opening and closing the
connection, creating the schema if necessary and inserting/updating
records.  Methods are idempotent and can be called repeatedly without
failure.

If you prefer to use SQLAlchemy instead of the built‑in connector,
adapt the implementation accordingly.  Using a connection pool is
recommended for production deployments.
"""

from __future__ import annotations

import logging
import datetime as _dt
from typing import Any, Dict, Iterable, List, Optional

from .config import Config

try:
    import mysql.connector  # type: ignore
except ImportError:
    # The mysql connector is optional; users must install it via
    # ``pip install mysql-connector-python``.  We defer the import error
    # until connect() is called to allow unit testing without MySQL.
    mysql = None  # type: ignore


LOGGER = logging.getLogger(__name__)


class Database:
    """Simple wrapper around the MySQL connector.

    Example usage::

        cfg = Config.from_env()
        db = Database(cfg)
        db.connect()
        db.init_schema()
        db.insert_jobs([...])
        db.close()

    """

    def __init__(self, config: Config) -> None:
        self.config = config
        self._conn: Optional[mysql.connector.MySQLConnection] = None  # type: ignore

    def connect(self) -> None:
        """Open a connection to the MySQL server.

        :raises ImportError: If the mysql connector is not installed.
        :raises mysql.connector.Error: For connection failures.
        """
        if mysql is None:
            raise ImportError(
                "mysql-connector-python is required to use the Database module"
            )
        LOGGER.debug("Connecting to MySQL at %s:%d", self.config.mysql_host, self.config.mysql_port)
        self._conn = mysql.connector.connect(
            host=self.config.mysql_host,
            port=self.config.mysql_port,
            user=self.config.mysql_user,
            password=self.config.mysql_password,
            database=self.config.mysql_database,
            autocommit=True,
        )
        LOGGER.info("Connected to MySQL database %s", self.config.mysql_database)

    def close(self) -> None:
        """Close the database connection if open."""
        if self._conn:
            LOGGER.debug("Closing MySQL connection")
            self._conn.close()
            self._conn = None

    def _cursor(self):  # type: ignore
        if not self._conn:
            raise RuntimeError("Database connection is not established; call connect() first")
        return self._conn.cursor()

    def init_schema(self) -> None:
        """Create tables if they do not already exist."""
        cur = self._cursor()
        # jobs table: store YARN application metadata
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                cluster_name VARCHAR(64) NOT NULL,
                app_id VARCHAR(64) NOT NULL,
                user VARCHAR(64),
                name VARCHAR(255),
                queue VARCHAR(64),
                state VARCHAR(32),
                final_status VARCHAR(32),
                started_time BIGINT,
                finished_time BIGINT,
                elapsed_time BIGINT,
                memory_seconds BIGINT,
                vcore_seconds BIGINT,
                queue_usage_pct FLOAT,
                cluster_usage_pct FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (cluster_name, app_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
        # spark_metrics table: store per-executor metrics
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS spark_metrics (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                cluster_name VARCHAR(64) NOT NULL,
                app_id VARCHAR(64) NOT NULL,
                executor_id VARCHAR(64),
                metric_name VARCHAR(128),
                metric_value DOUBLE,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (cluster_name, app_id) REFERENCES jobs(cluster_name, app_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
        # recommendations table: store analysis and tuning suggestions
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS recommendations (
                cluster_name VARCHAR(64) NOT NULL,
                app_id VARCHAR(64) NOT NULL,
                category VARCHAR(32),
                avg_cpu DOUBLE,
                avg_memory DOUBLE,
                status VARCHAR(32),
                recommended_executors INT,
                recommended_memory DOUBLE,
                recommended_cores INT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (cluster_name, app_id),
                FOREIGN KEY (cluster_name, app_id) REFERENCES jobs(cluster_name, app_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
        cur.close()
        LOGGER.info("Database schema initialised")

    def insert_jobs(self, cluster_name: str, jobs: Iterable[Dict[str, Any]]) -> None:
        """Insert or update job metadata records for a given cluster.

        :param cluster_name: Name of the cluster from which these jobs were fetched.
        :param jobs: Iterable of dictionaries containing job fields (see
          ``init_schema``).  Unknown keys are ignored.  When a record
          already exists, it will be updated.
        """
        cur = self._cursor()
        sql = (
            """
            INSERT INTO jobs
                (cluster_name, app_id, user, name, queue, state, final_status,
                 started_time, finished_time,
                 elapsed_time, memory_seconds, vcore_seconds, queue_usage_pct, cluster_usage_pct)
            VALUES
                (%(cluster_name)s, %(id)s, %(user)s, %(name)s, %(queue)s, %(state)s, %(finalStatus)s,
                 %(startedTime)s, %(finishedTime)s, %(elapsedTime)s, %(memorySeconds)s,
                 %(vcoreSeconds)s, %(queueUsagePercentage)s, %(clusterUsagePercentage)s)
            ON DUPLICATE KEY UPDATE
                user = VALUES(user),
                name = VALUES(name),
                queue = VALUES(queue),
                state = VALUES(state),
                final_status = VALUES(final_status),
                started_time = VALUES(started_time),
                finished_time = VALUES(finished_time),
                elapsed_time = VALUES(elapsed_time),
                memory_seconds = VALUES(memory_seconds),
                vcore_seconds = VALUES(vcore_seconds),
                queue_usage_pct = VALUES(queue_usage_pct),
                cluster_usage_pct = VALUES(cluster_usage_pct);
            """
        )
        count = 0
        for job in jobs:
            # Normalise keys for compatibility
            record: Dict[str, Any] = {
                "cluster_name": cluster_name,
                "id": job.get("id"),
                "user": job.get("user"),
                "name": job.get("name"),
                "queue": job.get("queue"),
                "state": job.get("state"),
                "finalStatus": job.get("finalStatus"),
                "startedTime": job.get("startedTime"),
                "finishedTime": job.get("finishedTime"),
                "elapsedTime": job.get("elapsedTime"),
                "memorySeconds": job.get("memorySeconds"),
                "vcoreSeconds": job.get("vcoreSeconds"),
                "queueUsagePercentage": job.get("queueUsagePercentage"),
                "clusterUsagePercentage": job.get("clusterUsagePercentage"),
            }
            cur.execute(sql, record)
            count += 1
        cur.close()
        LOGGER.debug("Inserted/updated %d job records for cluster %s", count, cluster_name)

    def insert_spark_metrics(self, cluster_name: str, app_id: str, metrics: Iterable[Dict[str, Any]]) -> None:
        """Insert executor metrics for a given application and cluster.

        :param cluster_name: Name of the cluster.
        :param app_id: YARN application ID.
        :param metrics: Iterable of metrics dictionaries with keys
          ``executor_id``, ``metric_name`` and ``metric_value``.
        """
        cur = self._cursor()
        sql = (
            """
            INSERT INTO spark_metrics (cluster_name, app_id, executor_id, metric_name, metric_value)
            VALUES (%s, %s, %s, %s, %s);
            """
        )
        count = 0
        for m in metrics:
            cur.execute(sql, (cluster_name, app_id, m["executor_id"], m["metric_name"], m["metric_value"]))
            count += 1
        cur.close()
        LOGGER.debug("Inserted %d spark metrics records for %s/%s", count, cluster_name, app_id)

    def upsert_recommendation(
        self,
        cluster_name: str,
        app_id: str,
        category: str,
        avg_cpu: Optional[float],
        avg_memory: Optional[float],
        status: str,
        recommended_executors: Optional[int],
        recommended_memory: Optional[float],
        recommended_cores: Optional[int],
        notes: Optional[str],
    ) -> None:
        """Insert or update recommendation for a job."""
        cur = self._cursor()
        sql = (
            """
            INSERT INTO recommendations
                (cluster_name, app_id, category, avg_cpu, avg_memory, status,
                 recommended_executors, recommended_memory, recommended_cores, notes)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                category = VALUES(category),
                avg_cpu = VALUES(avg_cpu),
                avg_memory = VALUES(avg_memory),
                status = VALUES(status),
                recommended_executors = VALUES(recommended_executors),
                recommended_memory = VALUES(recommended_memory),
                recommended_cores = VALUES(recommended_cores),
                notes = VALUES(notes);
            """
        )
        cur.execute(
            sql,
            (
                cluster_name,
                app_id,
                category,
                avg_cpu,
                avg_memory,
                status,
                recommended_executors,
                recommended_memory,
                recommended_cores,
                notes,
            ),
        )
        cur.close()
        LOGGER.debug("Upserted recommendation for %s/%s", cluster_name, app_id)

    def fetch_jobs(self, cluster_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return jobs stored in the database.

        If ``cluster_name`` is provided, only return jobs for that cluster.

        The returned dictionaries include the same keys as the YARN
        application object (lower‑case names) plus a Pythonic ``id`` key and
        ``cluster_name``.
        """
        cur = self._cursor()
        if cluster_name:
            cur.execute(
                """
                SELECT cluster_name, app_id AS id, user, name, queue, state, final_status AS finalStatus,
                       started_time AS startedTime, finished_time AS finishedTime,
                       elapsed_time AS elapsedTime, memory_seconds AS memorySeconds,
                       vcore_seconds AS vcoreSeconds, queue_usage_pct AS queueUsagePercentage,
                       cluster_usage_pct AS clusterUsagePercentage
                FROM jobs
                WHERE cluster_name = %s;
                """,
                (cluster_name,),
            )
        else:
            cur.execute(
                """
                SELECT cluster_name, app_id AS id, user, name, queue, state, final_status AS finalStatus,
                       started_time AS startedTime, finished_time AS finishedTime,
                       elapsed_time AS elapsedTime, memory_seconds AS memorySeconds,
                       vcore_seconds AS vcoreSeconds, queue_usage_pct AS queueUsagePercentage,
                       cluster_usage_pct AS clusterUsagePercentage
                FROM jobs;
                """
            )
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        cur.close()
        return [dict(zip(columns, row)) for row in rows]
