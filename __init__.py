from dagster import Definitions
from jobs import pipeline
from pipeline_sched import daily_3am_schedule

defs = Definitions(
    jobs=[pipeline],
    schedules=[daily_3am_schedule],
)
