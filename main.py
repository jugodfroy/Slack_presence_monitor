import psycopg2
from dotenv import load_dotenv
import os
from slack_sdk import WebClient
from datetime import datetime
import pytz


def configure():
    load_dotenv()
    print('Environment variables loaded\n')


def rds_connect():
    # Connect to RDS
    host = os.getenv('db_host')
    username = os.getenv('db_username')
    password = os.getenv('db_password')
    conn = psycopg2.connect(host=host, user=username, password=password)
    print('Successfully to RDS\n')
    return conn


def rds_create_table(conn):
    # Create table if not exists
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sleeq.public.slack_user_presence (
        id SERIAL PRIMARY KEY,
        user_id VARCHAR(255),
        user_email VARCHAR(255),
        presence VARCHAR(255),
        timestamp TIMESTAMP
    )
    """)
    conn.commit()
    cur.close()
    print('Table created if not existed\n')


def rds_insert_data(conn, data):
    # Insert data into RDS
    cur = conn.cursor()
    cur.executemany("""
    INSERT INTO sleeq.public.slack_user_presence (user_id, user_email, presence, timestamp)
    VALUES (%s, %s, %s, %s)
    """, data)
    conn.commit()
    cur.close()
    print('Data inserted\n')


def connect_slack():
    client = WebClient(token=os.getenv('slack_user_token'))
    print("\n Successfully connect to Slack API\n")
    return client


def get_user_id(slack_client):
    # get user list from slack api
    response = slack_client.users_list()

    # create a dictionary of user id and email
    user_id_dict = {}
    for user in response.data['members']:
        if user['deleted'] == False and user['is_bot'] == False:
            try:
                user_id_dict[user['id']] = user['profile']['email']
            except KeyError:
                user_id_dict[user['id']] = user['name']
    print('User ID and Email fetched:')
    print(user_id_dict)
    return user_id_dict


def get_user_presence(slack_client, user_id_dict):
    data = []

    # create datetime timestamp UTC+1
    paris_tz = pytz.timezone('Europe/Paris')
    timestamp = datetime.now(paris_tz)

   # get user presence from slack api for each user from dict
    for id in user_id_dict:
        response = slack_client.users_getPresence(user=id)
        data.append(
            [id, user_id_dict[id], response.data['presence'], timestamp])
    print('\nUser presence fetched:')
    print(data)
    return data


def main(event, context):
    print('\n###### Process started ######\n')
    configure()
    slack_client = connect_slack()
    user_id_dict = get_user_id(slack_client)
    data = get_user_presence(slack_client, user_id_dict)
    conn = rds_connect()
    rds_create_table(conn)
    rds_insert_data(conn, data)
    conn.close()
    print('\n###### Process finished ######\n')


if __name__ == '__main__':
    main("blank", "blank")
