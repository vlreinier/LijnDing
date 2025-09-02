from lijnding import Pipeline, stage, aggregator_stage, branch

# --- Stages for composition tests ---
@stage
def to_lower(text: str):
    return text.lower()

@stage
def split_words(text: str):
    return text.split()

@aggregator_stage
def count_items(items: list):
    """Counts the number of items in a list."""
    yield len(list(items))

def test_nested_pipeline():
    """Tests that a Pipeline can be used as a stage in another pipeline."""
    # Define a reusable sub-pipeline
    text_cleaning_pipeline = to_lower | split_words

    # Use the sub-pipeline as a single stage in the main pipeline
    main_pipeline = text_cleaning_pipeline | count_items

    data = ["Hello World", "This is LijnDing"]
    results, _ = main_pipeline.collect(data)

    # The inner pipeline will yield ['hello', 'world', 'this', 'is', 'lijnding']
    # The count_items aggregator will receive this full list and yield its length.
    assert results == [5]

def test_branch_with_nested_pipeline():
    """Tests that a branch can contain a full sub-pipeline."""
    # A stage that counts words in a sentence
    @aggregator_stage
    def word_count_agg(lines):
        yield len(list(lines)[0].split())

    # A stage that counts characters in a sentence
    @aggregator_stage
    def char_count_agg(lines):
        yield len(list(lines)[0])

    # A branch that runs both aggregators
    analysis_branch = branch(
        word_count_agg,
        char_count_agg,
        merge="zip"
    )

    pipeline = stage(lambda x: x) | analysis_branch

    data = ["Test ONE", "Test TWO"]
    results, _ = pipeline.collect(data)

    assert results == [(2, 8), (2, 8)]
