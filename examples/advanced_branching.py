"""
An example demonstrating the use of the Branch component for complex workflows.
"""
from lijnding import Pipeline, stage, Branch

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

    # This branch will apply to_upper and reverse_text to each item.
    # The results will be concatenated into a single stream.
    concat_branch = Branch(
        to_upper,
        reverse_text,
        merge="concat"  # 'concat' is the default, but we are explicit here
    )

    concat_pipeline = Pipeline() | concat_branch.to_stage()
    results_concat, _ = concat_pipeline.collect(data)

    print("Input:", data)
    print("Result:", results_concat)
    # Expected: ['HELLO', 'olleh', 'WORLD', 'dlrow', 'LIJNDING', 'gnidnjil']
    print("-" * 20)


    # --- Example 2: Zipped Branches ---
    print("\n--- Running Branch with 'zip' merge strategy ---")

    # This branch will apply three different operations to each item.
    # The results from each branch will be zipped together into tuples.
    zip_branch = Branch(
        to_upper,
        get_length,
        reverse_text,
        merge="zip"
    )

    zip_pipeline = Pipeline() | zip_branch.to_stage()
    results_zip, _ = zip_pipeline.collect(data)

    print("Input:", data)
    print("Result:", results_zip)
    # Expected: [('HELLO', 5, 'olleh'), ('WORLD', 5, 'dlrow'), ('LIJNDING', 8, 'gnidnjil')]
    print("-" * 20)


if __name__ == "__main__":
    main()
