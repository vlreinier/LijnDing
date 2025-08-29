"""
An example demonstrating the use of the Branch component for complex workflows.
"""
from lijnding import stage, branch

# --- Define some simple stages to be used in the branches ---
@stage
def to_upper(text: str) -> str:
    return text.upper()

@stage
def get_length(text: str) -> int:
    return len(text)

@stage
def reverse_text(text: str) -> str:
    return text[::-1]

def main():
    """Builds and runs pipelines with different branch merge strategies."""
    data = ["hello", "world", "lijnding"]

    # --- Example 1: Concatenated Branches ---
    print("--- Running Branch with 'concat' merge strategy ---")

    # Construct the pipeline directly using the `branch` factory function.
    # No need for .to_stage()
    concat_pipeline = to_upper | branch(reverse_text, get_length, merge="concat")

    results_concat, _ = concat_pipeline.collect(data)

    print("Input:", data)
    print("Result:", results_concat)
    print("-" * 20)


    # --- Example 2: Zipped Branches ---
    print("\n--- Running Branch with 'zip' merge strategy ---")

    # The `branch` component can be placed anywhere in the pipeline.
    zip_pipeline = branch(to_upper, get_length, reverse_text, merge="zip")

    results_zip, _ = zip_pipeline.collect(data)

    print("Input:", data)
    print("Result:", results_zip)
    print("-" * 20)


if __name__ == "__main__":
    main()
