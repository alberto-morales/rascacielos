from kafka import KafkaConsumer, TopicPartition
from webscraper. result_parser import ed_01

def main():
    # initializing resources
    print("Starting...")
    # code
    origin = 'MAD'
    destination = 'LCG'
    topic = origin + '-' + destination + '-htm'
    parser = ed_01()
    consumer = KafkaConsumer(bootstrap_servers=['certik:9092'],
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             auto_commit_interval_ms=1000,
                             group_id='parser-group',
                            )
    consumer.subscribe(topics=[topic])
    consumer.poll(0)
    # consumer.seek_to_beginning()
    consumer.seek(TopicPartition(topic, 0), 4)
    for msg in consumer:
        key = msg.key.decode('utf8')
        page_source = msg.value.decode('utf8')
        #
        # f = open("page_source.htm", "w", encoding='utf8')
        # f.write(page_source)
        # f.close()
        #
        print(key)

        df = None
        try:
            df = parser.parse(page_source)
            print(df)
        except Exception as e:
            print('EROR KE TE KAGAS ************************************************')
            print(e)
        print('-------------------------------------------------------------------------------------------------')


##code to execute in for loop goes here
if __name__ == '__main__':
    main()
