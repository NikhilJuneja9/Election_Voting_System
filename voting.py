import psycopg2
from confluent_kafka import Consumer , KafkaError, KafkaException, SerializingProducer
import simplejson as json
import random
import pytz
from main import delivery_report
import time

from datetime import datetime

conf = {
    'bootstrap.servers':'localhost:9092'
}

consumer = Consumer(conf | { # PIPE operator is used to merge two dict
    'group.id':'voting-group',
    'auto.offset.reset':'earliest',
    'enable.auto.commit':False
})

producer = SerializingProducer(conf=conf)

if __name__ ==  "__main__":
    
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres" )
    curr = conn.cursor()
    
    curr.execute(
    """ select row_to_json(cols) from (SELECT * FROM candidates) as cols
    
    """
    )
    candidates = [candidate[0] for candidate in curr.fetchall()]
    
    if len(candidates) ==0:
        raise Exception("No candidate found in the database")
    else:
        print(candidates)
        
    consumer.subscribe(['voters-topic'])
    try:
        while True:
            msg = consumer.poll(timeout = 1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF: # this error arises when the consumer is reached the end of the commit and there is no  more message 
                    continue
                else:
                    print(f"error arising in consumer{msg.error()}")
                    break
            else:
                print("consumer is working fine")
                voter = json.loads(msg.value().decode('utf-8'))
                chosen_candidate =  random.choice(candidates)
                print("candidate is chosen")
                vote =  voter | chosen_candidate | {
                    "voting_time": datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S"),
                    "vote":1
                }
                
                try:
                    print('user {} is voting for candidate {}'.format(vote['voter_id'], vote['candidate_id']))
                    curr.execute("""
                                 INSERT INTO votes(voter_id,candidate_id,voting_time)
                                 VALUES (%s, %s, %s)
                                 """,(vote['voter_id'], vote['candidate_id'],vote['voting_time']))
                    conn.commit()
                    
                    producer.produce(
                        topic='votes_topic',
                        key = vote['voter_id'],
                        value=json.dumps(vote),
                        on_delivery=delivery_report
                    )
                    producer.poll(0)
                except  Exception as e:
                    print(e)
            time.sleep(0.5)
    except Exception as e:
        print(e)
    