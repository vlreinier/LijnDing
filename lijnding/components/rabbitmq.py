from typing import Generator, Any, Iterable, Optional

try:
    import pika
    from pika.adapters.blocking_connection import BlockingChannel
except ImportError:
    pika = None
    BlockingChannel = None

from ..core.stage import stage, Stage
from ..core.errors import MissingDependencyError


def _check_pika():
    if pika is None:
        raise MissingDependencyError(
            "The 'pika' library is required to use RabbitMQ components. "
            "Please install it with: pip install 'lijnding[rabbitmq]'"
        )


def rabbitmq_source(
    *,
    host: str = 'localhost',
    queue: str,
    name: Optional[str] = None,
    **stage_kwargs,
) -> Stage:
    """
    Creates a source stage that consumes messages from a RabbitMQ queue.

    This is a source component, meaning it should be the first stage in a
    pipeline. It will connect to RabbitMQ, declare the queue to ensure it
    exists, and then yield messages as they are received. The stage will
    run until the pipeline is stopped.

    :param host: The hostname or IP address of the RabbitMQ server.
    :param queue: The name of the queue to consume from.
    :param name: An optional name for the stage.
    :param stage_kwargs: Additional keyword arguments to pass to the @stage decorator.
    :return: A new source `Stage` that yields messages from RabbitMQ.
    """
    _check_pika()
    stage_name = name or f"rabbitmq_source_{queue}"

    @stage(name=stage_name, stage_type="source", **stage_kwargs)
    def _rabbitmq_source_stage() -> Generator[bytes, None, None]:
        """The actual stage function that consumes from RabbitMQ."""
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        channel = connection.channel()

        channel.queue_declare(queue=queue, durable=True)

        try:
            # Note: This will block and consume indefinitely.
            # In a real-world scenario, you might want a more graceful way to stop.
            for method_frame, properties, body in channel.consume(queue):
                # Acknowledge the message
                channel.basic_ack(method_frame.delivery_tag)
                yield body
        finally:
            connection.close()

    return _rabbitmq_source_stage


def rabbitmq_sink(
    *,
    host: str = 'localhost',
    exchange: str,
    routing_key: str,
    name: Optional[str] = None,
    **stage_kwargs,
) -> Stage:
    """
    Creates a terminal stage that publishes all incoming items to a RabbitMQ exchange.

    This is an aggregator component that collects all items from the upstream
    pipeline and publishes them to the specified RabbitMQ exchange.

    :param host: The hostname or IP address of the RabbitMQ server.
    :param exchange: The name of the exchange to publish to.
    :param routing_key: The routing key to use when publishing.
    :param name: An optional name for the stage.
    :param stage_kwargs: Additional keyword arguments to pass to the @stage decorator.
    :return: A new `Stage` that publishes items to RabbitMQ.
    """
    _check_pika()
    stage_name = name or f"rabbitmq_sink_{exchange}_{routing_key}"

    @stage(name=stage_name, stage_type="aggregator", **stage_kwargs)
    def _rabbitmq_sink_stage(items: Iterable[Any]) -> None:
        """The actual stage function that publishes to RabbitMQ."""
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        channel = connection.channel()

        # The exchange should be declared by the publisher
        # channel.exchange_declare(exchange=exchange, exchange_type='direct', durable=True)

        try:
            for item in items:
                body = str(item).encode('utf-8') if not isinstance(item, bytes) else item
                channel.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=body,
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # make message persistent
                    ),
                )
        finally:
            connection.close()

    return _rabbitmq_sink_stage
