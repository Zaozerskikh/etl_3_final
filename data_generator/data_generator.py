from pymongo import MongoClient
from faker import Faker
import random
import uuid
from datetime import timedelta

fake = Faker()

client = MongoClient("mongodb://mongo:27017/")
db = client["mydatabase"]

user_sessions = []
for _ in range(1000):
    start_time = fake.date_time_this_year()
    end_time = start_time + timedelta(minutes=random.randint(1, 120))
    session = {
        "session_id": str(uuid.uuid4()),
        "user_id": fake.random_int(min=1, max=100),
        "start_time": start_time,
        "end_time": end_time,
        "pages_visited": [fake.url() for _ in range(random.randint(1, 5))],
        "device": fake.user_agent(),
        "actions": [fake.word() for _ in range(random.randint(1, 5))]
    }
    user_sessions.append(session)
db.UserSessions.insert_many(user_sessions)
print("user_sessions - ok")

print("all data - ok")