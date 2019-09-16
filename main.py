import sqlalchemy
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
  engine = sqlalchemy.create_engine(
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

def cleanup_event_date(event_date):
  try:
    cleaned_month_days = event_date \
      .replace(".??", ".01") \
      .replace("-??", "-01") \
      .replace("/??", "/01")
    return cleaned_month_days
  except:
    return None

def cleanup_headers(headers_dict):
  headers = {k.lower():j for k,j in dict(headers_dict).items()}
  for h in ['date','eventdate']:
    if h in headers:
      headers[h] = pd.to_datetime(cleanup_event_date(headers[h]), errors='coerce').date().strftime('%Y%m%d')
  for h in ['whiteelo', 'blackelo']:
    if h in headers:
      headers[h] = str(pd.to_numeric(headers[h], errors='coerce'))
  if 'round' in headers:
    if headers['round'].strip() == '?':
      headers['round'] = None
  return headers

def extract_headers_to_columns(in_df, columns_list):
  out_df = in_df.copy()
  for col in columns_list:
    out_df[col] = out_df['headers'].map(lambda x: None if col not in x else x[col])
  return out_df

def chessgames_to_df(games_list):
  df = pd.DataFrame()
  df['headers'] = list(map(lambda g: cleanup_headers(g.headers), games_list))
  df = extract_headers_to_columns(df, [
    'event',
    'site',
    'white',
    'black',
    'whiteelo',
    'blackelo',
    'round',
    'date',
    'eventdate',
    'eco'
  ])
  df['eventdate'] = pd.to_datetime(df['eventdate'].fillna(df['date']), errors='coerce')
  df['whiteelo'] = pd.to_numeric(df['whiteelo'], errors='coerce')
  df['blackelo'] = pd.to_numeric(df['blackelo'], errors='coerce')
  del df['date']
  df['pgn'] = list(map(lambda g: str(g.mainline()), games_list))
  return df

def load_games_to_db(df, conn):
  try:
    df.to_sql(
      "chessdb",
      conn,
      schema='public',
      if_exists='append',
      index=False,
      dtype={
        'event': sqlalchemy.types.String,
        'site': sqlalchemy.types.String,
        'white': sqlalchemy.types.String,
        'black': sqlalchemy.types.String,
        'whiteelo': sqlalchemy.types.Integer,
        'blackelo': sqlalchemy.types.Integer,
        'round': sqlalchemy.types.String,
        'eventdate': sqlalchemy.types.DATE,
        'pgn': sqlalchemy.types.String,
        'eco': sqlalchemy.types.String,
        'headers': sqlalchemy.types.JSON
      }
      )
  except:
    pass

if __name__=="__main__":
  conn = generate_psql_conn()
  games = list(filter(lambda x: x.endswith('.pgn'), os.listdir()))
  for filename in games:
    pgn_game_obs = extract_games_from_pgn(filename)
    df = chessgames_to_df(pgn_game_obs)
    load_games_to_db(df, conn)
    os.remove(filename)