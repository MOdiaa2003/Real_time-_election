<<<<<<< HEAD
import random
import time
from datetime import datetime,timedelta

import psycopg2
import simplejson as json
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer

polling_stations = [
    "School",
    "Community Center",
    "Library",
    "Religious Building",
    "Sports Arena",
    "Town Hall",
    "University Campus",
    "Government Building",
    "Post Office",
    "Fire Station",
    "Public Park"
]
voting_methods = [
    "In-person",
    "Mail-in",
    "Early Voting",
    "Online Voting",
    "Paper Ballot",
    "Electronic Voting Machine (EVM)"
]

# Define the education levels and their corresponding income categories
education_income_mapping = {
    "No Formal Education": "Low Income",
    "Primary School Graduate": "Low Income",
    "High School Graduate": "Lower-Middle Income",
    "Bachelor's Degree": "Middle Income",
    "Master's Degree": "Upper-Middle Income",
    "Doctoral Degree (PhD)": "High Income"
}
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


conf = {
    'bootstrap.servers': 'localhost:9092',
}

consumer = Consumer(conf | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

producer = SerializingProducer(conf)
def consume_messages():
    result = []
    consumer.subscribe(['candidates_topic'])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                result.append(json.loads(msg.value().decode('utf-8')))
                if len(result) == 3:
                    return result

            # time.sleep(5)
    except KafkaException as e:
        print(e)
# Function to generate a random voting time between February 2025 and May 2025
def generate_random_voting_time():
    start_date = datetime(2025, 2, 1)
    end_date = datetime(2025, 5, 31, 23, 59, 59)
    total_seconds = int((end_date - start_date).total_seconds())
    random_seconds = random.randint(0, total_seconds)
    random_datetime = start_date + timedelta(seconds=random_seconds)
    return random_datetime.strftime("%Y-%m-%d %H:%M:%S")        
if __name__ == "__main__":
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    # candidates
    cur.execute("""
        SELECT row_to_json(t)
        FROM (
            SELECT * FROM candidates
        ) t;
    """)
    candidates = cur.fetchall()
    candidates = [candidate[0] for candidate in candidates]

    if len(candidates) == 0:
        raise Exception("No candidates found in database")
    else:
        print(candidates)

    consumer.subscribe(['voters_topic'])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                voter = json.loads(msg.value().decode('utf-8'))
                chosen_candidate = random.choice(candidates)
                polling_station = random.choice(polling_stations)
                voting_method = random.choice(voting_methods)
                income_category=education_income_mapping.get(voter.get('education_level', ''), "Unknown")

                vote = voter | chosen_candidate | {
                "voting_time":generate_random_voting_time(),
                "voting_method": voting_method,
                "polling_station": polling_station,
                "vote": 1,
                "income_category": income_category  # Assign income category based on education level
            }

                try:
                    print("User {} is voting for candidate: {}".format(vote['voter_id'], vote['candidate_id']))
                    cur.execute("""
                        INSERT INTO votes (voter_id, candidate_id, voting_time, voting_method, polling_station,income_category)
                        VALUES (%s, %s, %s, %s, %s,%s)
                    """, (vote['voter_id'], vote['candidate_id'], vote['voting_time'],
                        vote['voting_method'], vote['polling_station'],vote['income_category']))
                    
                    conn.commit()

                    producer.produce(
                        'votes_topic',
                        key=vote["voter_id"],
                        value=json.dumps(vote),
                        on_delivery=delivery_report
                    )
                    producer.poll(0)
                    #time.sleep(2)
                except Exception as e:
                    print("Error: {}".format(e))
                    conn.rollback()
                    continue

    except KafkaException as e:
        print(e)
=======
import random
import time
from datetime import datetime,timedelta

import psycopg2
import simplejson as json
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer

polling_stations = [
    "School",
    "Community Center",
    "Library",
    "Religious Building",
    "Sports Arena",
    "Town Hall",
    "University Campus",
    "Government Building",
    "Post Office",
    "Fire Station",
    "Public Park"
]
voting_methods = [
    "In-person",
    "Mail-in",
    "Early Voting",
    "Online Voting",
    "Paper Ballot",
    "Electronic Voting Machine (EVM)"
]

# Define the education levels and their corresponding income categories
education_income_mapping = {
    "No Formal Education": "Low Income",
    "Primary School Graduate": "Low Income",
    "High School Graduate": "Lower-Middle Income",
    "Bachelor's Degree": "Middle Income",
    "Master's Degree": "Upper-Middle Income",
    "Doctoral Degree (PhD)": "High Income"
}
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


conf = {
    'bootstrap.servers': 'localhost:9092',
}

consumer = Consumer(conf | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

producer = SerializingProducer(conf)
def consume_messages():
    result = []
    consumer.subscribe(['candidates_topic'])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                result.append(json.loads(msg.value().decode('utf-8')))
                if len(result) == 3:
                    return result

            # time.sleep(5)
    except KafkaException as e:
        print(e)
# Function to generate a random voting time between February 2025 and May 2025
def generate_random_voting_time():
    start_date = datetime(2025, 2, 1)
    end_date = datetime(2025, 5, 31, 23, 59, 59)
    total_seconds = int((end_date - start_date).total_seconds())
    random_seconds = random.randint(0, total_seconds)
    random_datetime = start_date + timedelta(seconds=random_seconds)
    return random_datetime.strftime("%Y-%m-%d %H:%M:%S")        
if __name__ == "__main__":
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    # candidates
    cur.execute("""
        SELECT row_to_json(t)
        FROM (
            SELECT * FROM candidates
        ) t;
    """)
    candidates = cur.fetchall()
    candidates = [candidate[0] for candidate in candidates]

    if len(candidates) == 0:
        raise Exception("No candidates found in database")
    else:
        print(candidates)

    consumer.subscribe(['voters_topic'])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                voter = json.loads(msg.value().decode('utf-8'))
                chosen_candidate = random.choice(candidates)
                polling_station = random.choice(polling_stations)
                voting_method = random.choice(voting_methods)
                income_category=education_income_mapping.get(voter.get('education_level', ''), "Unknown")

                vote = voter | chosen_candidate | {
                "voting_time":generate_random_voting_time(),
                "voting_method": voting_method,
                "polling_station": polling_station,
                "vote": 1,
                "income_category": income_category  # Assign income category based on education level
            }

                try:
                    print("User {} is voting for candidate: {}".format(vote['voter_id'], vote['candidate_id']))
                    cur.execute("""
                        INSERT INTO votes (voter_id, candidate_id, voting_time, voting_method, polling_station,income_category)
                        VALUES (%s, %s, %s, %s, %s,%s)
                    """, (vote['voter_id'], vote['candidate_id'], vote['voting_time'],
                        vote['voting_method'], vote['polling_station'],vote['income_category']))
                    
                    conn.commit()

                    producer.produce(
                        'votes_topic',
                        key=vote["voter_id"],
                        value=json.dumps(vote),
                        on_delivery=delivery_report
                    )
                    producer.poll(0)
                    #time.sleep(2)
                except Exception as e:
                    print("Error: {}".format(e))
                    conn.rollback()
                    continue

    except KafkaException as e:
        print(e)
>>>>>>> cfe031446c7ebab50941df42ad4dfb8949e9e8a5
