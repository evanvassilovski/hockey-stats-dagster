from dagster import ScheduleDefinition
from jobs import pipeline

def daily_config():
    return {
        "ops": {
            "load_schedule": {"config": {"table_name": "schedule"}},
            "load_skater_stats": {"config": {"table_name": "skater_stats"}},
            "load_goalie_stats": {"config": {"table_name": "goalie_stats"}},
        }
    }

daily_3am_schedule = ScheduleDefinition(
    job=pipeline,
    cron_schedule="0 3 * * *",  # Runs every day at 3 AM
    execution_timezone="US/Eastern",  # Eastern time zone
    run_config=daily_config()  # ← THIS is what was missing
)