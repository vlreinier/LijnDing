"""
This example demonstrates how to use the RabbitMQ source and sink components.

To run this example, you need a running RabbitMQ server. You can start one
easily using Docker:
    docker run -d --hostname my-rabbit --name some-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management

You will also need to install the 'pika' library:
    pip install 'lijnding[rabbitmq]'

The example is split into two parts:
1. A 'producer' script that sends messages to a queue.
2. A 'consumer' pipeline that reads from the queue, processes the messages,
   and sends the results to another queue.
"""
import time
import threading

from lijnding.core import stage, Pipeline
from lijnding.components import rabbitmq_source, rabbitmq_sink


# --- Part 1: Producer ---
# This is a simple script to send some messages to our task queue.
# We use the 'pika' library directly for this.
def produce_messages():
    import pika
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        # Declare a direct exchange and two queues
        channel.exchange_declare(exchange='lijnding_exchange', exchange_type='direct', durable=True)
        channel.queue_declare(queue='task_queue', durable=True)
        channel.queue_declare(queue='result_queue', durable=True)

        # Bind the queues to the exchange
        channel.queue_bind(exchange='lijnding_exchange', queue='task_queue', routing_key='task')
        channel.queue_bind(exchange='lijnding_exchange', queue='result_queue', routing_key='result')

        print("Producer: Sending messages...")
        for i in range(5):
            message = f"hello world {i}"
            channel.basic_publish(
                exchange='lijnding_exchange',
                routing_key='task',
                body=message.encode('utf-8'),
                properties=pika.BasicProperties(delivery_mode=2) # make message persistent
            )
            print(f"Producer: Sent '{message}'")
            time.sleep(0.5)

        connection.close()
        print("Producer: Done.")
    except ImportError:
        print("\nCould not run producer: 'pika' is not installed.")
        print("Please run: pip install 'lijnding[rabbitmq]'")
    except pika.exceptions.AMQPConnectionError:
        print("\nCould not connect to RabbitMQ. Is it running?")
        print("You can start a test instance with Docker:")
        print("docker run -d --hostname my-rabbit --name some-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management")


# --- Part 2: Consumer Pipeline ---
# This is a lijnding pipeline that processes messages from RabbitMQ.

# Define a simple processing stage
@stage
def to_upper(data: bytes) -> str:
    return data.decode('utf-8').upper()


# Build the pipeline
# 1. Read from 'task_queue'
# 2. Convert message body to uppercase
# 3. Print the result to the console
# 4. Send the result to 'result_queue'
processing_pipeline = Pipeline(
    rabbitmq_source(queue='task_queue'),
    to_upper,
    stage(print),
    rabbitmq_sink(exchange='lijnding_exchange', routing_key='result')
)


if __name__ == "__main__":
    print("--- lijnding RabbitMQ Example ---")

    # Run the producer in a separate thread
    producer_thread = threading.Thread(target=produce_messages)
    producer_thread.start()

    # Give the producer a moment to start and send messages
    print("\nConsumer: Starting pipeline to process messages...")
    print("Consumer: Waiting for messages from 'task_queue'...")
    print("Consumer: The pipeline will run until manually stopped (Ctrl+C).")

    # Run the pipeline.
    # The rabbitmq_source will block and listen for messages indefinitely.
    # In a real application, you would run this as a long-running service.
    # For this example, it will process the 5 messages from the producer
    # and then wait.
    try:
        # We run with a limit for this example to make it exit automatically.
        # In a real service, you would likely omit the `limit`.
        processing_pipeline.run(limit=5)
        print("\nConsumer: Pipeline finished after processing 5 messages.")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
        print("Please ensure RabbitMQ is running and the 'pika' library is installed.")

    producer_thread.join()
    print("\n--- Example Finished ---")
