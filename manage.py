from x_sql import my_sql
from producer import Producer
from consumer import  do_consumer


sql = my_sql()
def main(start,end, fakeid):
    """创建表  生产  消费"""
    sql.create_table()
    #生产
    do_producer = Producer(start,end, fakeid)
    do_producer.run()
    #消费
    do_consumer()


if __name__ == "__main__":
    main(0, 276, "MjM5MDA1MTA1MQ==")
    sql.close()