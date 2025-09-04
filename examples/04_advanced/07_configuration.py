"""
An advanced example demonstrating how to use the configuration system.
"""

from lijnding.core import stage
from lijnding.core.context import Context


@stage
def add_greeting(context: Context, text: str) -> str:
    """
    A stage that adds a greeting to the input text.
    The greeting is sourced from the pipeline's configuration.
    """
    # Access the config object from the context.
    # The .get() method allows providing a default value if the key is not found.
    greeting = context.config.get("processing.greeting", "DefaultGreeting")
    return f"{greeting}, {text}"


@stage
def punctuate(context: Context, text: str) -> str:
    """
    A stage that adds punctuation to the input text.
    The punctuation is sourced from the pipeline's configuration.
    """
    punc = context.config.get("processing.punctuation", ".")
    return f"{text}{punc}"


# 1. Define the pipeline. The stages are generic and not tied to specific
#    config values.
pipeline = add_greeting | punctuate


def main():
    """
    Builds and runs a pipeline, demonstrating the use of external configuration.
    """
    data = ["world", "Jules"]
    config_path = "examples/config/default.yml"

    # 2. Run the pipeline WITH the configuration file.
    #    The `collect` method will load the YAML file and make it available
    #    to the stages via the context.
    print(f"--- Running pipeline with configuration from: {config_path} ---")
    results, _ = pipeline.collect(data, config_path=config_path)

    print("\n--- Results (with config) ---")
    print(results)
    print("Expected: ['Hello from config, world!!!', 'Hello from config, Jules!!!']")

    # 3. Run the pipeline WITHOUT the configuration file.
    #    This demonstrates that the stages fall back to their default values
    #    gracefully when the config is not provided.
    print("\n--- Running pipeline without configuration ---")
    results_no_config, _ = pipeline.collect(data)

    print("\n--- Results (no config) ---")
    print(results_no_config)
    print("Expected: ['DefaultGreeting, world.', 'DefaultGreeting, Jules.']")


if __name__ == "__main__":
    main()
