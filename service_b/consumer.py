from kafka import KafkaConsumer
import json

# הגדרת ה-Consumer שיתחבר ל-Kafka
consumer = KafkaConsumer(
    'message_topic',  # שם ה-topic שמהם נשאבו ההודעות
    bootstrap_servers='localhost:9092',
    group_id='message_group',  # זיהוי קבוצת הצרכנים
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # המרת המידע בחזרה לפורמט json
)

# האזנה להודעות ומדפיסות אותן
for message in consumer:
    print(f"Received message: {message.value}")
