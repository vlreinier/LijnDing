"""
An example demonstrating how pipelines can be nested and composed.
"""
from lijnding import Pipeline, stage

# --- Define a reusable sub-pipeline ---
# This pipeline takes a string, cleans it, and splits it into words.
@stage
def to_lower(text: str):
    return text.lower()

@stage
def remove_punctuation(text: str):
    return "".join(c for c in text if c.isalpha() or c.isspace())

@stage
def split_words(text: str):
    return text.split()

# A reusable pipeline that encapsulates a common sequence of tasks.
text_cleaning_pipeline = to_lower | remove_punctuation | split_words
text_cleaning_pipeline.name = "TextCleaner" # Give it a name for clarity


# --- Define the main pipeline ---
@stage
def read_line(line: str):
    """A dummy stage that just yields the line."""
    print(f"Processing line: '{line.strip()}'")
    yield line

@stage
def count_items(items: list):
    """Counts the number of items in a list."""
    return len(items)


def main():
    """Builds and runs the main pipeline which uses the nested pipeline."""

    data = [
        "This is the first line. It has punctuation!",
        "Here is a second, shorter line.",
    ]

    # Construct the main pipeline.
    # The `text_cleaning_pipeline` is treated as a single stage.
    main_pipeline = read_line | text_cleaning_pipeline | count_items

    print("--- Running main pipeline with nested pipeline ---")
    results, _ = main_pipeline.collect(data)

    print("\n--- Results ---")
    print("Word counts for each line:", results)


if __name__ == "__main__":
    main()
