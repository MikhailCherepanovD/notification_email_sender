from confluent_kafka import Consumer, KafkaException, KafkaError
import smtplib
import configparser
import asyncio
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

config = configparser.ConfigParser()
config.read("config.ini")

kafka_host = config["KAFKA"]["HOST"]
kafka_port = int(config["KAFKA"]["PORT"])
kafka_topic = config["KAFKA"]["TOPIC"]
kafka_group = config["KAFKA"]["GROUP"]

smtp_host = config['SMTP']['HOST']
smtp_port = int(config['SMTP']['PORT'])

email_address = config['EMAIL']['ADDRESS']
email_password = config['EMAIL']['PASSWORD']

kafka_conf = {
    'bootstrap.servers': f'{kafka_host}:{kafka_port}',
    'group.id': kafka_group,
    'auto.offset.reset': 'latest'
}
consumer = Consumer(kafka_conf)
consumer.subscribe([kafka_topic])

smtpObj = smtplib.SMTP(smtp_host, smtp_port)
smtpObj.starttls()
smtpObj.login(email_address, email_password)

async def send_message(destination_email_address, msg) -> None:
    subject = 'Информация по билету'
    try:
        message = MIMEMultipart()
        message["From"] = email_address
        message["To"] = destination_email_address
        message["Subject"] = subject  # Добавляем тему письма
        message.attach(MIMEText(msg, "plain", "utf-8"))  # Кодировка UTF-8
        await asyncio.to_thread(smtpObj.sendmail, email_address, destination_email_address, message.as_string())
        print(f"Message has been sent to {destination_email_address}")
    except Exception as e:
        print(f"Error sending to {destination_email_address}: {e}")

def get_prepared_message(parsed_json):
    def get_transfers(transfers):
        return "Без пересадок" if transfers == 0 else "С пересадками"

    return (f"Текущая информация по маршруту "
            f"{parsed_json['ticketData']['origin']} -> {parsed_json['ticketData']['destination']} \n"
            f"Дата/время отправления: {parsed_json['ticketData']['departure_at']}\n\n"
            f"Цена: {parsed_json['ticketData']['price']} руб;\n"
            f"{get_transfers(parsed_json['ticketData']['transfers'])}; \n"
            f"Билет: {parsed_json['ticketData']['link']}")


def get_email_address_and_prepared_message(msg: str) -> tuple:
    try:
        parsed_json = json.loads(msg)
        email = parsed_json.get("email", "")
        if not email:
            return "", ""
        return email, get_prepared_message(parsed_json)
    except json.JSONDecodeError as e:
        print(f"Error parsing message to JSON: {e}")
        return "", ""


async def listen_kafka():
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # читаем сообщение
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            msg_str = msg.value().decode("utf-8")
            print("Message received")
            email, prepared_message = get_email_address_and_prepared_message(msg_str)

            await send_message(email, prepared_message)

    except Exception as e:
        print("Error in listen_kafka:", e)
    finally:
        consumer.close()

async def main():
    await listen_kafka()

asyncio.run(main())
