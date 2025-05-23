from dagster import ScheduleDefinition
from jobs import pipeline

daily_3am_schedule = ScheduleDefinition(
    job=pipeline,
    cron_schedule="0 3 * * *",  # This is standard cron for 3:00 AM daily
    execution_timezone="US/Eastern"  # or your preferred timezone
)