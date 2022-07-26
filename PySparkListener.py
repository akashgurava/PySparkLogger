from dataclasses import dataclass
import datetime
import time

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.java_gateway import JavaGateway, ensure_callback_server_started


@dataclass
class SparkJob:
    """
    A Dataclass to represent SparkJob.
    """

    start_repr = "(SparkJob ID:{}. Start:{}. Status: Started.)"
    end_repr = "(SparkJob ID:{}. Duration: {}. Start:{}. End: {}. Status: Completed.)"

    job_id: int
    start_ts: datetime.datetime
    end_ts: datetime.datetime
    start: float
    end: float = 0

    @property
    def duration(self) -> str:
        if self.end == 0:
            return "NOT_COMPLETED"
        return time.strftime("%H:%M:%S", time.gmtime(self.end - self.start))

    @classmethod
    def from_now(cls, job_id) -> "SparkJob":
        now = datetime.datetime.now()
        return SparkJob(job_id=job_id, start_ts=now, end_ts=now, start=time.monotonic())

    def __hash__(self) -> int:
        return hash(self.job_id)

    def __repr__(self) -> str:
        if self.end == 0:
            return self.start_repr.format(
                self.job_id, self.start_ts.strftime("%H:%M:%S")
            )
        return self.end_repr.format(
            self.job_id,
            self.duration,
            self.start_ts.strftime("%H:%M:%S"),
            self.end_ts.strftime("%H:%M:%S"),
        )


class PySparkListener(object):
    def __init__(self):
        self.app_start = time.monotonic()
        self.app_end = None
        self.jobs: dict[int, SparkJob] = {}

    # Application level events
    def onApplicationEnd(self, app_end):
        self.app_end = time.monotonic()
        print("Application End.")

    def onApplicationStart(self, app_start):
        print("Application Start.")

    # Job level events
    def onJobStart(self, job_start):
        job_id = job_start.jobId()
        job = SparkJob.from_now(job_id)
        self.jobs[job_id] = job
        print(job)

    def onJobEnd(self, job_end):
        job_id = job_end.jobId()
        job = self.jobs.get(job_id) or SparkJob.from_now(job_id)
        job.end = time.monotonic()
        job.end_ts = datetime.datetime.now()
        print(job)

    # Stage level events
    def onStageSubmitted(self, stageSubmitted):
        pass

    def onStageCompleted(self, stageCompleted):
        pass

    # Task level events
    def onTaskStart(self, taskStart):
        pass

    def onTaskEnd(self, taskEnd):
        pass

    # Executor events
    def onExecutorAdded(self, executorAdded):
        pass

    def onExecutorRemoved(self, executorRemoved):
        pass

    def onTaskGettingResult(self, taskGettingResult):
        pass

    def onBlockManagerRemoved(self, blockManagerRemoved):
        pass

    def onBlockUpdated(self, blockUpdated):
        pass

    def onEnvironmentUpdate(self, environmentUpdate):
        pass

    def onExecutorMetricsUpdate(self, executorMetricsUpdate):
        pass

    def onOtherEvent(self, event):
        pass

    def onUnpersistRDD(self, unpersistRDD):
        pass

    class Java:
        implements = ["org.apache.spark.scheduler.SparkListenerInterface"]


spark = SparkSession.builder.getOrCreate()
sc: SparkContext = spark.sparkContext
gateway: JavaGateway = sc._gateway or JavaGateway()

ensure_callback_server_started(gateway)
listener = PySparkListener()
sc._jsc.sc().addSparkListener(listener)  # type: ignore

df = spark.range(100)
df.count()
spark.stop()
gateway.shutdown_callback_server()
