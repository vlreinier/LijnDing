"""
An example demonstrating the pipeline visualization feature.
"""

from lijnding.core import stage
from lijnding.components.branch import branch

# --- Define some stages for a complex pipeline ---


@stage
def source():
    """A source stage that yields some initial data."""
    yield from ["hello", "world", "visualization"]


@stage
def to_upper(text: str):
    """Converts text to uppercase."""
    return text.upper()


def add_punctuation(punc: str):
    """A factory for a stage that adds punctuation."""

    @stage(name=f"add_{punc.strip().replace('!', 'exclamation')}")
    def _add_punc(text: str):
        return f"{text}{punc}"

    return _add_punc


@stage
def get_length(text: str):
    """Calculates the length of a string."""
    return len(text)


def main():
    """
    Builds a complex pipeline and generates a DOT graph visualization.
    """
    # --- Build a pipeline with branches and nested pipelines ---

    # A simple pipeline that will be nested
    punctuate_pipeline = add_punctuation("!") | add_punctuation("?")

    # A more complex pipeline with a branch
    main_pipeline = (
        source
        | to_upper
        | branch(get_length, punctuate_pipeline, merge="zip")
        | stage(name="format_output")(lambda x: f"Result: {x}")
    )
    main_pipeline.name = "Main Processing Pipeline"

    # --- Generate and print the visualization ---

    print("--- Pipeline Visualization (DOT Graph) ---")
    print()
    print("# To render this output, save it to a file (e.g., `pipeline.dot`)")
    print("# and run the command: dot -Tpng -o pipeline.png pipeline.dot")
    print("# (Requires Graphviz to be installed: https://graphviz.org/download/)")
    print()

    dot_graph = main_pipeline.visualize()
    print(dot_graph)


if __name__ == "__main__":
    main()
