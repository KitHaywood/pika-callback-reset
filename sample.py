import pika
from multiprocessing import Process,Pipe,Queue
import multiprocessing as mp
import schedule
import yaml
import json
import traceback
from datetime import datetime
import queue
import logging
from threading import Thread

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
    )

logging.getLogger("pika").setLevel(logging.ERROR)

def send_on_minute(instrs: list[str],
                   counterq: Queue,
                   outqueues: list[pika.frame.Method],
                   counter: dict) -> None: 
    # Deals with sending to rabbitmq on minute for consumption in operation container
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='rabbitmq',
                port=5672,
                credentials=pika.PlainCredentials(
                    username='guest',
                    password='guest'
                    ),
                )
            )
        channel = connection.channel()
        for q in outqueues:
            channel.queue_declare(queue=q)
            channel.basic_publish(
                exchange='',
                routing_key=q,
                body=json.dumps(counter) # counter sent to rabbit (5)
                )
        counter = {}
        for i in instrs:
            counter[i] = 0
        channel.close()
        connection.close()
    except:
        logging.info(f"[rate-limit] - [send_on_minute] - {traceback.format_exc()}")

def callback(body: str,
             counter: dict,
             counterq: Queue,
             resetq: Queue) -> None:
    try:
        counter = resetq.get(block=False) # trying to reset counter here
    except queue.Empty:
        pass
    msg = json.loads(body)
    if 'epic' in msg.keys(): # add to count if a) is a quote msg, and b) is of the correct IBAN
        counter[msg['epic']] += 1
    counterq.put(counter) # send back to infinite while loop in main (4)
    
def in_data_proc(q: pika.frame.Method,
                 counter: dict,
                 counterq: Queue,
                 resetq: Queue) -> None:
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='rabbitmq',
                port=5672,
                credentials=pika.PlainCredentials(
                    username='guest',
                    password='guest'
                    ),
                )
            )

        channel = connection.channel()
        logging.info('connection and channel made')
        channel.queue_declare(queue=q)
        logging.info(f'queue declared = {q}')
        
        channel.basic_consume(
            queue=q,
            on_message_callback=lambda ch, method, properties,body: \
                callback(
                    body,
                    counter=counter, # counter handed to callback (3)
                    counterq=counterq,
                    resetq=resetq
                    ),
            auto_ack=True
            )
        logging.info('consumption started')
        cons_thread = Thread(target=channel.start_consuming)
        channel.close()
        connection.close()
    except:
        logging.info(f"[{mp.current_process().name}] - [in_data_proc] - {traceback.format_exc()}")

def reset_queue(instrs: list[str],
                resetq: Queue) -> None:
    counter = {}
    for i in instrs:
        counter[i] = 0
    resetq.put(counter)
    
def main() -> None:
    try:
        counterq = Queue()
        resetq = Queue()
        
        with open('modelparams.yaml','r') as f:
            data = yaml.safe_load(f)

        instrs = list(set(list(data.keys())))
        outqueues = []
            
        for i in instrs:
            outqueues.append(f"{i}-ratelimiter-out")        
                
        counter = {}  # counter initialized here (1)
        for i in instrs:
            counter[i] = 0
            
        inq = 'ratelimiter-in'
        proc = Process(
            target=in_data_proc,
            args=(inq,counter,counterq,resetq), # counter handed as arg to process (2)
            name=inq
            )
        proc.start()    
        schedule.every().minute.at(f':00').do(lambda: send_on_minute(instrs,counterq,outqueues,counter)) # sends to rabbit for consumption in operation 
        schedule.every().minute.at(f':01').do(lambda: reset_queue(instrs, resetq)) # resets queue at 1 second past the minute to avoid potentiall overwriting the data
        while True:
            try:
                counter = counterq.get(block=False)
            except queue.Empty:
                pass
            schedule.run_pending()
    except:
        logging.info(traceback.format_exc())    
if __name__=="__main__":
    main()