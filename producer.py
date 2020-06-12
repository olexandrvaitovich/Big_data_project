import requests
import json
from kafka import KafkaProducer


def stream_generator(url):
    r = requests.get(url, stream=True)
    for jf in r.iter_lines():
        if jf:
            decoded_jf = jf.decode('utf-8')
            yield json.loads(decoded_jf)


def main():
    producer = KafkaProducer(bootstrap_servers='{ip1}:9092,{ip2}:9092,{ip3}:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    topic = 'stream_data'
    url = 'http://stream.meetup.com/2/rsvps'
    gen = stream_generator(url)

    while True:
        try:
            item = next(gen)
            future = producer.send(topic=topic, value=item)
            metadata = future.get(timeout=10)
            print(f'message sent: topic={metadata.topic}, offset={metadata.offset}, partition={metadata.partition}')
        except Exception as e:
            print(e)
        finally:
            producer.flush()

if __name__=='__main__':
    main()
