from sqlalchemy import create_engine
import json
import pandas as pd
import boto3
import chess.pgn
import os

def fetch_secrets():
  secret_name = 'postgres-kv'
  region_name = 'us-west-2'

  session = boto3.session.Session(profile_name='mysiteidea')
  client = session.client(
    service_name='secretsmanager',
    region_name=region_name
  )

  secrets_json = json.loads(
    client.get_secret_value(SecretId = secret_name)['SecretString']
    )
  return secrets_json

def generate_psql_conn():
  creds = fetch_secrets()
  engine = create_engine(
    'postgresql://{user}:{password}@{host}:{port}/{dbname}'.format(
        user=creds['username'],
        password=creds['password'],
        host=creds['host'],
        port=creds['port'],
        dbname='mysiteidea'
    )
  )
  return engine.connect()

def extract_games_from_pgn(pgn_file):
  try:
    pgn = open(pgn_file)

    games_list = []
    g = chess.pgn.read_game(pgn)
    while g:
      games_list.append(g)
      g = chess.pgn.read_game(pgn)
    return games_list
  except:
    return []

def chessgames_to_df(games_list):
  df = pd.DataFrame(map(lambda g: g.headers, games_list))
  df.columns = list(map(lambda c: c.strip().lower(), df.columns))
  df['eventdate'] = pd.to_datetime(df['eventdate'])
  df['gameresult'] = df['result']
  del df['result']
  df['pgn'] = list(map(lambda g: str(g.mainline()), games_list))
  col_order = [
    'event',
    'site',
    'eventdate',
    'round',
    'white',
    'black',
    'gameresult',
    'blackelo',
    'whiteelo',
    'eco',
    'pgn'
  ]
  return df[col_order]

def load_games_to_db(df, conn):
  df.to_sql("chessdb", conn, schema='public', if_exists='append', index=False) 

if __name__=="__main__":
  conn = generate_psql_conn()
  games = list(filter(lambda x: x.endswith('.pgn'), os.listdir()))
  for filename in games:
    pgn_game_obs = extract_games_from_pgn(filename)
    df = chessgames_to_df(pgn_game_obs)
    load_games_to_db(df, conn)
    os.remove(filename)