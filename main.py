import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from db import get_engine
from sqlalchemy import text

def query_to_dataframe(query, params=None):
    try:
        engine = get_engine()
        with engine.connect() as conn:
            raw_conn = conn.connection
            if params:
                return pd.read_sql_query(text(query), raw_conn, params=params)
            else:
                return pd.read_sql_query(query, raw_conn)
    except Exception as e:
        print(f"Database error: {str(e)}")
        return pd.DataFrame()

historic_urls = query_to_dataframe('SELECT "URL" FROM schedule')

# Normal
cur_date = datetime.now() - timedelta(days=1)
cur_date = cur_date.strftime('%Y%m%d')

team_codes = pd.DataFrame({
    'Codes':['VAN','EDM','CGY','SEA','SJ','ANA','LA','LV',
             'WPG','MIN','CHI','STL','NSH','DAL','COL','UTA',
             'MON','OTT','TOR','DET','BUF','BOS','TB','FLA',
             'CLB','PIT','PHI','NJ','NYR','NYI','WAS','CAR'],
    'Teams':['Vancouver','Edmonton','Calgary','Seattle','San Jose','Anaheim','Los Angeles','Vegas',
             'Winnipeg','Minnesota','Chicago','St. Louis','Nashville','Dallas','Colorado','Utah',
             'Montreal','Ottawa','Toronto','Detroit','Buffalo','Boston','Tampa Bay','Florida',
             'Columbus','Pittsburgh','Philadelphia','New Jersey','N.Y. Rangers','N.Y. Islanders','Washington','Carolina']
})

# Load Schedule
schedule_url = 'https://www.cbssports.com/nhl/schedule/' + cur_date + '/'

# Load Scores

def getScores(url, historic_urls):
    table = pd.read_html(url)[0]
    table = table[['Away', 'Home', 'Result']]
    conditions = [
        table['Result'].str.contains(r'\bOT\b', case=False, na=False),
        table['Result'].str.contains(r'\bSO\b', case=False, na=False)
    ]
    choices = [1, 2]

    table['Final_Code'] = np.select(conditions, choices, default=0)

    table['Result'] = table['Result'].str.replace(r' / (OT|SO)', '', regex=True)

    split_scores = table['Result'].str.split(' - ')

    score_df = split_scores.apply(pd.Series)
    score_df.columns = ['Win_Team', 'Lose_Team']

    score_df[['Win_Team_Name', 'Win_Team_Score']] = score_df['Win_Team'].str.extract(r'(\w+)\s+(\d+)')
    score_df[['Lose_Team_Name', 'Lose_Team_Score']] = score_df['Lose_Team'].str.extract(r'(\w+)\s+(\d+)')
    score_df = score_df[['Win_Team_Name', 'Win_Team_Score', 'Lose_Team_Name', 'Lose_Team_Score']]
    win_df = score_df[['Win_Team_Name', 'Win_Team_Score']].copy()
    win_df.columns=['Codes', 'Score']
    lose_df = score_df[['Lose_Team_Name', 'Lose_Team_Score']].copy()
    lose_df.columns=['Codes', 'Score']
    score_df = pd.concat([win_df, lose_df], ignore_index=True)

    table = pd.merge(table, team_codes.rename(columns={'Teams':'Away', 'Codes':'Away_Codes'}), on=['Away'], how='left')
    table = pd.merge(table, team_codes.rename(columns={'Teams':'Home', 'Codes':'Home_Codes'}), on=['Home'], how='left')

    table = pd.merge(table, score_df.rename(columns={'Score':'Away_Score', 'Codes':'Away_Codes'}), on=['Away_Codes'], how='left')
    table = pd.merge(table, score_df.rename(columns={'Score':'Home_Score', 'Codes':'Home_Codes'}), on=['Home_Codes'], how='left')
    table = table[['Away', 'Home', 'Away_Score', 'Home_Score', 'Final_Code', 'Away_Codes', 'Home_Codes']]
    table['Date'] = cur_date

    table['URL'] = 'https://www.cbssports.com/nhl/gametracker/boxscore/NHL_' + table['Date'] + '_' + table['Away_Codes'] + '@' + table['Home_Codes'] + '/'
    table.columns
    table = table.drop(columns=['Away_Codes', 'Home_Codes'])
    if len(historic_urls) > 0:
        table = table[table['URL'].isin(historic_urls['URL'])==False]
    return table

# daily_schedule = getScores(schedule_url, historic_urls)

def getSkaterStats(table):
    # Get Box Score Data
    if len(table) > 0:
        urls = table['URL']
        daily_skaters = pd.DataFrame({})

        for link in urls:
            tables = pd.read_html(link)
            tables = tables[1:4:2]
            road_skaters = tables[0]
            home_skaters = tables[1]
            index = table[table['URL'] == link].index
            away_team = table['Away'][index].values[0]
            home_team = table['Home'][index].values[0]
            road_skaters['Team'] = away_team
            road_skaters['Opponent'] = home_team
            road_skaters['Date'] = cur_date
            home_skaters['Team'] = home_team
            home_skaters['Opponent'] = away_team
            home_skaters['Date'] = cur_date
            skaters = pd.concat([home_skaters, road_skaters], ignore_index=True)

            # Separate FW/FL
            faceoffs = skaters['FW/FL'].str.split('/')
            faceoffs = faceoffs.apply(pd.Series)
            faceoffs.columns = ['FW', 'FL']

            fo_index = skaters.columns.get_loc('FW/FL')

            skaters = skaters.drop(columns=['FW/FL'])
            skaters.insert(fo_index, 'FW', faceoffs['FW'])
            skaters.insert(fo_index + 1, 'FL', faceoffs['FL'])

            # Change TOI to a float
            toi = skaters['TOI'].str.split(':')
            toi = toi.apply(pd.Series)
            toi.columns = ['Mins', 'Secs']
            toi[['Mins', 'Secs']] = toi[['Mins', 'Secs']].apply(pd.to_numeric, errors='coerce')
            toi['TOI'] = (toi['Mins'] + (toi['Secs'] / 60)).round(2)
            toi_index = skaters.columns.get_loc('TOI')
            skaters = skaters.drop(columns=['TOI'])
            skaters.insert(toi_index, 'TOI', toi['TOI'])

            pos = skaters['SKATERS'].str.extract(r'\b(LW|C|RW|D)$')
            skaters.insert(1, 'Position', pos)

            skaters['SKATERS'] = skaters['SKATERS'].str.replace(r'\s+(LW|C|RW|D)', '', regex=True)

            skaters['URL'] = link
            skaters = skaters.rename(columns={'SKATERS':'Skater', 'HITS':'Hits'})
            skaters[['FW', 'FL']] = skaters[['FW', 'FL']].apply(pd.to_numeric, errors='coerce')
            
            daily_skaters = pd.concat([daily_skaters, skaters], ignore_index=True)
    else:
        daily_skaters = pd.DataFrame({})


    return daily_skaters


def getGoalieStats(table):
    # Get Box Score Data
    if len(table) > 0:
        urls = table['URL']
        daily_goalies = pd.DataFrame({})

        for link in urls:
            tables = pd.read_html(link)
            tables = tables[5:8:2]
            road_goalies = tables[0]
            home_goalies = tables[1]
            index = table[table['URL'] == link].index
            away_team = table['Away'][index].values[0]
            home_team = table['Home'][index].values[0]
            road_goalies['Team'] = away_team
            road_goalies['Opponent'] = home_team
            road_goalies['Date'] = cur_date
            home_goalies['Team'] = home_team
            home_goalies['Opponent'] = away_team
            home_goalies['Date'] = cur_date

            goalies = pd.concat([home_goalies, road_goalies], ignore_index=True)

            # Change TOI to a float
            toi = goalies['TOI'].str.split(':')
            toi = toi.apply(pd.Series)
            toi.columns = ['Mins', 'Secs']
            toi[['Mins', 'Secs']] = toi[['Mins', 'Secs']].apply(pd.to_numeric, errors='coerce')
            toi['TOI'] = (toi['Mins'] + (toi['Secs'] / 60)).round(2)
            toi_index = goalies.columns.get_loc('TOI')
            goalies = goalies.drop(columns=['TOI'])
            goalies.insert(toi_index, 'TOI', toi['TOI'])

            goalies['URL'] = link
            goalies = goalies.rename(columns={'GOALIES':'Goalie'})

            daily_goalies = pd.concat([daily_goalies, goalies], ignore_index=True)
            if len(historic_urls) > 0:
                daily_goalies[daily_goalies['URL'].isin(historic_urls['URL'])==False]
    else:
        daily_goalies = pd.DataFrame({})
    
    return daily_goalies

# daily_skaters = getSkaterStats(daily_schedule)
# daily_goalies = getGoalieStats(daily_schedule)