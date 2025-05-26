import pandas as pd
from db import get_engine
from main import getScores, getSkaterStats, getGoalieStats, query_to_dataframe
from dagster import job, op, Field, String, execute_job
from sqlalchemy import MetaData, Table, text

@op
def get_schedule_data() -> pd.DataFrame:
    cur_date = pd.Timestamp.now() - pd.Timedelta(days=1)
    cur_date = cur_date.strftime('%Y%m%d')
    schedule_url = f'https://www.cbssports.com/nhl/schedule/{cur_date}/'
    historic_urls = query_to_dataframe('SELECT "URL" FROM schedule')
    return getScores(schedule_url, historic_urls)

@op
def get_skater_stats(schedule_data: pd.DataFrame) -> pd.DataFrame:
    return getSkaterStats(schedule_data)

@op
def get_goalie_stats(schedule_data: pd.DataFrame) -> pd.DataFrame:
    return getGoalieStats(schedule_data)

@op(config_schema={"table_name": Field(String)})
def load_to_db(context, df: pd.DataFrame):
    table_name = context.op_config["table_name"]
    if df.empty:
        context.log.info(f"Skipping load - empty DataFrame for {table_name}")
        return
    try:
        engine = get_engine()
        with engine.begin() as conn:
            # Reflect the existing table structure
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=engine)
            
            # Insert data
            conn.execute(
                table.insert(),
                df.to_dict('records')
            )
        context.log.info(f"Successfully loaded data to {table_name}")
    except Exception as e:
        context.log.error(f"Error loading data to {table_name}: {str(e)}")
        raise
    # df.to_sql(table_name, engine, if_exists='append', index=False)

load_schedule = load_to_db.alias("load_schedule")
load_skater_stats = load_to_db.alias("load_skater_stats")
load_goalie_stats = load_to_db.alias("load_goalie_stats")

@job
def pipeline():
    schedule = get_schedule_data()
    skater_stats = get_skater_stats(schedule)
    goalie_stats = get_goalie_stats(schedule)
    load_schedule(schedule)
    load_skater_stats(skater_stats)
    load_goalie_stats(goalie_stats)

if __name__ == "__main__":
    result = pipeline.execute_in_process(
        run_config={
            "ops": {
                "load_schedule": {"config": {"table_name": "schedule"}},
                "load_skater_stats": {"config": {"table_name": "skater_stats"}},
                "load_goalie_stats": {"config": {"table_name": "goalie_stats"}},
            }
        }
    )