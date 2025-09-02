import pytest
from unittest.mock import MagicMock, patch, call

from lijnding import Pipeline, stage
from lijnding.components import rabbitmq_source, rabbitmq_sink
from lijnding.core.errors import MissingDependencyError

# Define fake data to be "consumed" from RabbitMQ
FAKE_MESSAGES = [
    (MagicMock(delivery_tag=1), None, b"message 1"),
    (MagicMock(delivery_tag=2), None, b"message 2"),
    (MagicMock(delivery_tag=3), None, b"message 3"),
]


@pytest.fixture
def mock_pika():
    """A pytest fixture to mock the pika library."""
    with patch("lijnding.components.rabbitmq.pika") as mock_pika_lib:
        # Configure the mock connection and channel
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_pika_lib.BlockingConnection.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        yield mock_pika_lib, mock_connection, mock_channel


def test_rabbitmq_source(mock_pika):
    """
    Tests the rabbitmq_source component with a mocked pika library.
    """
    mock_pika_lib, mock_connection, mock_channel = mock_pika
    mock_channel.consume.return_value = iter(FAKE_MESSAGES)

    # Define a simple pipeline
    source = rabbitmq_source(queue="test_queue")
    p = source | stage(lambda x: x.decode().upper())

    from itertools import islice

    # Run the pipeline and collect results
    # Source stages ignore the input data, so we can pass an empty list.
    stream, _ = p.run(data=[])
    try:
        # We use islice to take a limited number of items from the potentially
        # infinite stream from the source.
        results = list(islice(stream, 3))
    finally:
        # Explicitly close the stream to ensure the generator's finally block
        # is executed, which closes the connection.
        if hasattr(stream, 'close'):
            stream.close()

    # Assertions
    assert results == ["MESSAGE 1", "MESSAGE 2", "MESSAGE 3"]

    # Verify that pika was used correctly
    mock_pika_lib.BlockingConnection.assert_called_once_with(
        mock_pika_lib.ConnectionParameters(host='localhost')
    )
    mock_connection.channel.assert_called_once()
    mock_channel.queue_declare.assert_called_once_with(queue="test_queue", durable=True)
    mock_channel.consume.assert_called_once_with("test_queue")

    # Check that messages were acknowledged
    assert mock_channel.basic_ack.call_count == 3
    mock_channel.basic_ack.assert_has_calls([
        call(1), call(2), call(3)
    ])

    # Verify the connection was closed
    mock_connection.close.assert_called_once()


def test_rabbitmq_sink(mock_pika):
    """
    Tests the rabbitmq_sink component with a mocked pika library.
    """
    mock_pika_lib, mock_connection, mock_channel = mock_pika

    # Define a pipeline that sends data to the sink
    # We need a source stage for the pipeline to be runnable
    p = stage(lambda x: x) | rabbitmq_sink(exchange="test_exchange", routing_key="test_key")
    input_data = ["data 1", "data 2"]

    # Run the pipeline. We must consume the output stream to ensure
    # the aggregator (the sink) is executed.
    output_stream, _ = p.run(input_data)
    list(output_stream)  # Consume the stream

    # Assertions
    mock_pika_lib.BlockingConnection.assert_called_once_with(
        mock_pika_lib.ConnectionParameters(host='localhost')
    )
    mock_connection.channel.assert_called_once()

    # Verify that basic_publish was called for each item
    assert mock_channel.basic_publish.call_count == 2
    mock_channel.basic_publish.assert_has_calls([
        call(
            exchange="test_exchange",
            routing_key="test_key",
            body=b"data 1",
            properties=mock_pika_lib.BasicProperties(delivery_mode=2),
        ),
        call(
            exchange="test_exchange",
            routing_key="test_key",
            body=b"data 2",
            properties=mock_pika_lib.BasicProperties(delivery_mode=2),
        ),
    ])

    # Verify the connection was closed
    mock_connection.close.assert_called_once()


def test_missing_pika_dependency():
    """
    Tests that a MissingDependencyError is raised if pika is not installed.
    """
    # Simulate pika not being installed by patching it to be None
    with patch("lijnding.components.rabbitmq.pika", None):
        with pytest.raises(MissingDependencyError, match="pip install 'lijnding\\[rabbitmq\\]'"):
            rabbitmq_source(queue="any")

        with pytest.raises(MissingDependencyError, match="pip install 'lijnding\\[rabbitmq\\]'"):
            rabbitmq_sink(exchange="any", routing_key="any")
