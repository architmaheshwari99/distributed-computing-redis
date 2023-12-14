import random
from json import loads

import redis

import config

def redis_db():
    db = redis.Redis(
        host=config.redis_host,
        port=config.redis_port,
        db=config.redis_db_number,
        # password=config.redis_password,
        decode_responses=True,
    )
    db.ping()

    return db


def redis_queue_push(db, message):
    print(f"pushed in {config.redis_queue_name}")
    db.lpush(config.redis_queue_name, message)

def redis_queue_pop(db):
    # the 'b' in brpop indicates this is a blocking call, only one worker can
    # fetch the message from the queue. This is not the case with SQS, multiple delivery
    # is possible in SQS.
    print("poping")
    _, message_json = db.brpop(config.redis_queue_name)
    print(db.keys())
    return message_json

def process_message(db, message_json: str):
    message = loads(message_json)
    print(f"Message received: id={message['id']}, message_number={message['data']['message_number']}")

    # mimic potential processing errors
    processed_ok = random.choices((True, False), weights=(5, 1), k=1)[0]
    if processed_ok:
        print(f"\tProcessed successfully")
    else:
        print(f"\tProcessing failed - requeuing...")
        redis_queue_push(db, message_json)


def main():
    """
    Generates `num_messages` and pushes them to a Redis queue
    :param num_messages:
    :return:
    """

    # connect to Redis
    db = redis_db()

    while True:
        message_json = redis_queue_pop(db)
        process_message(db, message_json)


if __name__ == "__main__":
    main()
    