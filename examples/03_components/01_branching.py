"""
An example demonstrating the use of the Branch component for complex workflows.
"""
from lijnding.core import stage
from lijnding.components.branch import branch

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

    # The `branch` component sends each incoming item to all of its child branches.
    # The results are then merged back into a single stream.

    # --- Example 1: Concatenated Branches ---
    # The `concat` merge strategy will flatten the results from all branches into a
    # single list.
    print("--- Running Branch with 'concat' merge strategy ---")

    # For each word in `data`, this pipeline will generate three results:
    # 1. The uppercase version of the word.
    # 2. The reversed version of the word.
    # 3. The length of the word.
    concat_pipeline = branch(to_upper, reverse_text, get_length, merge="concat")

    results_concat, _ = concat_pipeline.collect(data)

    print("Input:", data)
    print("Result (a flattened list of all results):")
    print(results_concat)
    # Expected: ['HELLO', 'olleh', 5, 'WORLD', 'dlrow', 5, 'LIJNDING', 'gnidnjil', 8]
    print("-" * 20)


    # --- Example 2: Zipped Branches ---
    # The `zip` merge strategy will group the results for each input item into a tuple.
    print("\n--- Running Branch with 'zip' merge strategy ---")

    # This pipeline will create a tuple for each word, containing the
    # uppercase version, the length, and the reversed version.
    zip_pipeline = branch(to_upper, get_length, reverse_text, merge="zip")

    results_zip, _ = zip_pipeline.collect(data)

    print("Input:", data)
    print("Result (a list of tuples):")
    print(results_zip)
    # Expected: [('HELLO', 5, 'olleh'), ('WORLD', 5, 'dlrow'), ('LIJNDING', 8, 'gnidnjil')]
    print("-" * 20)


if __name__ == "__main__":
    main()
