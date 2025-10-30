import csv
import random
from faker import Faker
from numpy.random import normal
from datetime import datetime, timedelta


fake = Faker()

NUM_TRANSACTIONS = 5000000
OUTPUT = 'transactions.csv'
HIGH_RISK_JURISDICTIONS = ['Country A', 'Country B', 'Country C']
ALL_JURISDICTIONS = HIGH_RISK_JURISDICTIONS + [fake.country() for _ in range(30)]

accounts = [fake.iban() for _ in range(50000)]

with open(OUTPUT, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([
        'transaction_id', 
        'from_account', 
        'to_account', 
        'amount', 
        'timestamp', 
        'to_jurisdiction'
    ])

    start_date = datetime(2024, 1, 1)

    for i in range(NUM_TRANSACTIONS):
        
        amount = round(abs(normal(loc=500, scale=1000))) + round(random.uniform(0, 99), 2)
        if random.random() < 0.001: 
            amount = round(random.uniform(10001, 100000), 2)
        
        ts = start_date + timedelta(
            days=random.randint(0, 364),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )

        writer.writerow([
            fake.uuid4(),
            random.choice(accounts),
            random.choice(accounts),
            amount,
            ts.isoformat(),
            random.choice(ALL_JURISDICTIONS)
        ])