#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: abekthink

import queue
import time
import traceback
from threading import Thread


class Producer(Thread):
    def __init__(self, queue_size=2048):
        Thread.__init__(self)
        self.queue = queue.Queue(maxsize=queue_size)

    def produce(self):
        pass

    def run(self):
        tasks = self.produce()
        if not tasks:
            return
        for task in tasks:
            self.queue.put(task)
        return

    def sync(self, qsize=0, delay_time=5):
        while self.queue.qsize() > qsize:
            # sleep delay_time seconds
            # must use gevent.sleep(), not use time.sleep()
            time.sleep(delay_time)


class Consumer(Thread):
    def __init__(self, queue, queue_timeout=30):
        Thread.__init__(self)
        self.queue = queue
        self.queue_timeout = queue_timeout

    def consume(self, task):
        pass

    def run(self):
        while True:
            try:
                task = self.queue.get(timeout=self.queue_timeout)
            except queue.Empty:
                cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                print('consumer: the consumer get timeout, and then quit normally at %s' % cur_time)
                break

            try:
                self.consume(task)
            except:
                traceback.print_stack()

            if getattr(self, '_exit_', False):
                break


if __name__ == "__main__":
    class TestProducer(Producer):
        def __init__(self, queue_size=2048, name="producer"):
            Producer.__init__(self, queue_size)

        def produce(self):
            print("producer: begin to produce")
            for i in range(0, 20):
                yield i
            self.sync(delay_time=1)

            for i in range(100, 120):
                yield i
            self.sync(delay_time=1)

            print("producer: end to produce")


    class TestConsumer(Consumer):
        def __init__(self, queue, queue_timeout=5, name="consumer"):
            Consumer.__init__(self, queue, queue_timeout)
            self.name = name

        def consume(self, task):
            print("consumer: the name is %s, consume the data {%d}" % (self.name, task))


    print("main: begin...")
    thread_count = 4
    producer = TestProducer()
    producer.start()
    for i in range(thread_count):
        consumer = TestConsumer(producer.queue, name="consumer%d" % i)
        consumer.start()
    print("main: end...")
