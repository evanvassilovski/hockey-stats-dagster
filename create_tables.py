from sqlalchemy import Table, Column, String, Date, MetaData, Integer, Float, PrimaryKeyConstraint
from db import engine

metadata = MetaData()

schedule = Table("schedule", metadata,
    Column("Away", String),
    Column("Home", String),
    Column("Away_Score", Integer),
    Column("Home_Score", Integer),
    Column("Final_Code", Integer),
    Column("Date", Date),
    Column("URL", String, primary_key=True)
)

skater_stats = Table("skater_stats", metadata,
    Column("Skater", String, primary_key=True),
    Column("Position", String),
    Column("G", Integer),
    Column("A", Integer),
    Column("+/-", Integer),
    Column("SOG", Integer),
    Column("FW", Integer),
    Column("FL", Integer),
    Column("PIM", Integer),
    Column("TOI", Float),
    Column("Hits", Integer),
    Column("Team", String),
    Column("Opponent", String),
    Column("Date", Date),
    Column("URL", String, primary_key=True),
    PrimaryKeyConstraint("Skater", "URL")
)

gpalie_stats = Table("goalie_stats", metadata,
    Column("Goalie", String, primary_key=True),
    Column("SA", Integer),
    Column("GA", Integer),
    Column("SV", Integer),
    Column("SV%", Float),
    Column("TOI", Float),
    Column("Team", String),
    Column("Opponent", String),
    Column("Date", Date),
    Column("URL", String, primary_key=True),
    PrimaryKeyConstraint("Goalie", 'URL')
)

metadata.create_all(engine)