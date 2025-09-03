from lijnding.core import stage, Pipeline
from lijnding.core.context import Context


def test_pipeline_uses_config_file(tmp_path):
    """
    Tests that a pipeline can correctly load and use parameters from a
    config file provided to the `collect` method.
    """
    # 1. Define a stage that depends on a configuration value
    @stage
    def multiply_by(context: Context, item: int) -> int:
        multiplier = context.config.get("processing.multiplier", 1)
        return item * multiplier

    # 2. Create the configuration file
    config_content = """
    processing:
      multiplier: 10
    """
    config_file = tmp_path / "config.yml"
    config_file.write_text(config_content)

    # 3. Build and run the pipeline with the config path
    pipeline = multiply_by
    data = [1, 2, 3]
    results, _ = pipeline.collect(data, config_path=str(config_file))

    # 4. Assert that the config value was used
    assert results == [10, 20, 30]


def test_pipeline_uses_defaults_without_config_file():
    """
    Tests that the same pipeline falls back to default values when no
    config path is provided.
    """
    # 1. Define the same stage as the previous test
    @stage
    def multiply_by(context: Context, item: int) -> int:
        multiplier = context.config.get("processing.multiplier", 1)
        return item * multiplier

    # 2. Build and run the pipeline WITHOUT the config path
    pipeline = multiply_by
    data = [1, 2, 3]
    results, _ = pipeline.collect(data)

    # 3. Assert that the default value (1) was used
    assert results == [1, 2, 3]


def test_pipeline_uses_defaults_with_empty_config(tmp_path):
    """
    Tests that the pipeline falls back to defaults if the config file
    is empty or doesn't contain the required key.
    """
    @stage
    def multiply_by(context: Context, item: int) -> int:
        multiplier = context.config.get("processing.multiplier", 5)
        return item * multiplier

    # Create an empty config file
    config_file = tmp_path / "config.yml"
    config_file.write_text("")

    pipeline = multiply_by
    data = [1, 2, 3]
    results, _ = pipeline.collect(data, config_path=str(config_file))

    # Assert that the default value from the stage (5) was used
    assert results == [5, 10, 15]
