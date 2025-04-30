
import random
import psycopg2
import requests
import simplejson as json
from confluent_kafka import SerializingProducer

BASE_URL = 'https://randomuser.me/api/?nat=gb'
PARTIES = ["Management Party", "Savior Party", "Tech Republic Party"]

education_levels = [
    "No Formal Education",
    "Primary School Graduate",
    "High School Graduate",
    "Bachelor's Degree",
    "Master's Degree",
    "Doctoral Degree (PhD)"
]
random.seed(42)


def generate_voter_data():
    response = requests.get('https://randomuser.me/api/?nat=gb&results=5000')
    if response.status_code == 200:
        users = response.json()['results']
        voter_data_list = []
        
        for user in users:
           
            education_level = random.choice(education_levels)
            
            voter_data = {
                "voter_id": user['login']['uuid'],
                "voter_name": f"{user['name']['first']} {user['name']['last']}",
                "date_of_birth": user['dob']['date'],
                "gender": user['gender'],
                "nationality": user['nat'],
                "registration_number": user['login']['username'],
                "address": {
                    "street": f"{user['location']['street']['number']} {user['location']['street']['name']}",
                    "city": user['location']['city'],
                    "state": user['location']['state'],
                    "country": user['location']['country'],
                    "postcode": user['location']['postcode']
                },
                "email": user['email'],
                "phone_number": user['phone'],
                "cell_number": user['cell'],
                "picture": user['picture']['large'],
                "registered_age": user['registered']['age'],
                "party": random.choice(PARTIES),
                "education_level": education_level
            }
            voter_data_list.append(voter_data)

        return voter_data_list
    else:
        return "Error fetching data"

def generate_candidate_data(candidate_number, total_parties):
    response = requests.get(BASE_URL + '&gender=' + ('female' if candidate_number % 2 == 1 else 'male'))
    if response.status_code == 200:
        user_data = response.json()['results'][0]


        return {
            "candidate_id": user_data['login']['uuid'],
            "candidate_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "party_affiliation": PARTIES[candidate_number % total_parties],
            "biography": "A brief bio of the candidate.",
            "campaign_platform": "Key campaign promises or platform.",
            "photo_url": user_data['picture']['large']
        }
    else:
        return "Error fetching data"


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


# Kafka Topics
voters_topic = 'voters_topic'
candidates_topic = 'candidates_topic'


def create_tables(conn, cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            biography TEXT,
            campaign_platform TEXT,
            photo_url TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
    voter_id VARCHAR(255) PRIMARY KEY,
    voter_name VARCHAR(255),
    date_of_birth VARCHAR(255),
    gender VARCHAR(255),
    nationality VARCHAR(255),
    registration_number VARCHAR(255),
    address_street VARCHAR(255),
    address_city VARCHAR(255),
    address_state VARCHAR(255),
    address_country VARCHAR(255),
    address_postcode VARCHAR(255),
    email VARCHAR(255),
    phone_number VARCHAR(255),
    cell_number VARCHAR(255),
    education_level VARCHAR(255),
    party VARCHAR(255),
    picture TEXT,
    registered_age INTEGER
)

    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            voting_method VARCHAR(255),
            polling_station VARCHAR(255),
            income_category VARCHAR(255),    
            vote int DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)

    conn.commit()


def insert_voters(conn, cur, voter):
    cur.execute("""
        INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number,
        address_street, address_city, address_state, address_country, address_postcode, email, phone_number,
        cell_number, education_level, party, picture, registered_age)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
    (voter["voter_id"], voter['voter_name'], voter['date_of_birth'], voter['gender'],
     voter['nationality'], voter['registration_number'], voter['address']['street'],
     voter['address']['city'], voter['address']['state'], voter['address']['country'],
     voter['address']['postcode'], voter['email'], voter['phone_number'],
     voter['cell_number'], voter['education_level'], voter['party'], voter['picture'], voter['registered_age'])
    )
    conn.commit()



if __name__ == "__main__":
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()


    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092', })
    create_tables(conn, cur)

    # get candidates from db
    cur.execute("""
        SELECT * FROM candidates
    """)
    candidates = cur.fetchall()
    print(candidates)

    if len(candidates) == 0:
        for i in range(3):
            candidate = generate_candidate_data(i, 3)
            print(candidate)
            cur.execute("""
                        INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                candidate['candidate_id'], candidate['candidate_name'], candidate['party_affiliation'], candidate['biography'],
                candidate['campaign_platform'], candidate['photo_url']))
            conn.commit()

    for i in range(5):

        all_voters = generate_voter_data()

        for i, voter_data in enumerate(all_voters):
            insert_voters(conn, cur, voter_data)

            producer.produce(
                voters_topic,
                key=voter_data["voter_id"],
                value=json.dumps(voter_data),
                on_delivery=delivery_report
            )

            print('Produced voter {}, data: {}'.format(i, voter_data))

        producer.flush()
