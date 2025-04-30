# # # import requests
# # # import csv
# # # import time 
# # # BASE_URL = 'https://randomuser.me/api/?results=5000&nat=gb'

# # # def fetch_voters():
# # #     response = requests.get(BASE_URL)
# # #     if response.status_code == 200:
# # #         users = response.json()['results']
# # #         voters = []
# # #         for user_data in users:
# # #             voters.append({
# # #                 "voter_id": user_data['login']['uuid'],
# # #                 "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
# # #                 "age": user_data['dob']['age'],
# # #                 "city": user_data['location']['city'],
# # #                 "date_of_birth": user_data['dob']['date'],
# # #                 "gender": user_data['gender'],
# # #                 "nationality": user_data['nat'],
# # #                 "registration_number": user_data['login']['username'],
# # #                 "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
# # #                 "state": user_data['location']['state'],
# # #                 "country": user_data['location']['country'],
# # #                 "postcode": user_data['location']['postcode'],
# # #                 "email": user_data['email'],
# # #                 "phone_number": user_data['phone'],
# # #                 "cell_number": user_data['cell'],
# # #                 "picture": user_data['picture']['large'],
# # #                 "registered_age": user_data['registered']['age']
# # #             })
# # #         return voters
# # #     else:
# # #         print("Failed to fetch data:", response.status_code)
# # #         return []

# # # # Append voters 10 times
# # # for _ in range(10):  # Repeat the process 10 times
# # #     voters = fetch_voters()
# # #     time .sleep(2)
# # #     if voters:
# # #         with open("voters_5000.csv", mode="a", newline="", encoding="utf-8") as file:  # Open in append mode
# # #             writer = csv.DictWriter(file, fieldnames=voters[0].keys())
# # #             if file.tell() == 0:  # Check if the file is empty, then write header
# # #                 writer.writeheader()
# # #             writer.writerows(voters)
# # #         print(f"Appended {len(voters)} voters to voters_5000.csv")



# # import csv
# # import time
# # import json
# # import psycopg2
# # from confluent_kafka import SerializingProducer

# # # Kafka & DB config
# # CSV_PATH = r"F:/DE_Projects/Real_time _election/voters_5000.csv"
# # voters_topic = 'voters_topic'

# # conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
# # cur = conn.cursor()

# # producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})


# # def insert_voter_row(cur, voter):
# #     cur.execute("""
# #         INSERT INTO voters (
# #             voter_id, voter_name, date_of_birth, gender, nationality, registration_number,
# #             address_street, address_city, address_state, address_country, address_postcode,
# #             email, phone_number, cell_number, picture, registered_age
# #         )
# #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
# #         ON CONFLICT (voter_id) DO NOTHING
# #     """, (
# #         voter["voter_id"], voter["voter_name"], voter["date_of_birth"], voter["gender"],
# #         voter["nationality"], voter["registration_number"], voter["address_street"],
# #         voter["address_city"], voter["address_state"], voter["address_country"], voter["address_postcode"],
# #         voter["email"], voter["phone_number"], voter["cell_number"], voter["picture"], voter["registered_age"]
# #     ))


# # def delivery_report(err, msg):
# #     if err is not None:
# #         print(f'Message delivery failed: {err}')
# #     else:
# #         print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


# # # 🧩 Read & Process
# # with open(CSV_PATH, mode='r', encoding='utf-8') as csvfile:
# #     reader = csv.DictReader(csvfile)
# #     for i, row in enumerate(reader):
# #         voter_data = {
# #             "voter_id": row["voter_id"],
# #             "voter_name": row["voter_name"],
# #             "date_of_birth": row["date_of_birth"],
# #             "gender": row["gender"],
# #             "nationality": row["nationality"],
# #             "registration_number": row["registration_number"],
# #             "address_street": row["address_street"],
# #             "address_city": row["address_city"],
# #             "address_state": row["address_state"],
# #             "address_country": row["address_country"],
# #             "address_postcode": row["address_postcode"],
# #             "email": row["email"],
# #             "phone_number": row["phone_number"],
# #             "cell_number": row["cell_number"],
# #             "picture": row["picture"],
# #             "registered_age": int(row["registered_age"])
# #         }

# #         # Insert to DB
# #         start_postgres = time.time()
# #         insert_voter_row(cur, voter_data)
# #         conn.commit()
# #         print(f"[{i}] ⏱️ Postgres insert time: {time.time() - start_postgres:.4f}s")

# #         # Produce to Kafka
# #         start_kafka = time.time()
# #         producer.produce(
# #             topic=voters_topic,
# #             key=voter_data["voter_id"],
# #             value=json.dumps(voter_data),
# #             on_delivery=delivery_report
# #         )
# #         print(f"[{i}] ⏱️ Kafka produce time: {time.time() - start_kafka:.4f}s")

# # producer.flush()
# # print("✅ Done processing all rows.")

# import pandas as pd
# import psycopg2

# # 🔵 Step 1: Read table from source Postgres
# source_conn = psycopg2.connect("host=localhost dbname=candidates user=postgres password=postgres")
# df = pd.read_sql_query("SELECT * FROM public.voters", source_conn)
# df.to_csv("candidates_table_backup.csv", index=False)
# source_conn.close()
# print("✅ Table exported from source Postgres and saved as CSV.")
