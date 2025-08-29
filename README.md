# LijnDing: A Composable Pipeline Framework

LijnDing is a lightweight, type-aware, and composable pipeline framework for Python. It allows you to build complex data processing workflows by chaining together simple, reusable stages.

## Key Features

- **Simple & Composable API**: Use the `@stage` decorator and the `|` operator to build elegant, readable pipelines.
- **Multiple Backends**: Run your pipelines serially, in a thread pool for I/O-bound tasks, or (in a future release) across multiple processes for CPU-bound tasks.
- **Error Handling**: Configure how your pipeline behaves on errors with policies like `fail`, `skip`, or `retry`.
- **Branching & Merging**: Create complex DAG-style workflows with parallel branches that can be merged back together.
- **Automatic Context Injection**: Share state and metrics across your pipeline with a `Context` object that is automatically injected into stages that need it.

## Basic Usage

Here's a simple example of building a pipeline to process text:

```python
# examples/simple_pipeline.py
from lijnding import Pipeline, stage

# 1. Define pipeline stages using the @stage decorator
@stage
def split_sentences(text: str):
    """Takes a block of text and yields individual sentences."""
    for sentence in text.split('.'):
        cleaned_sentence = sentence.strip()
        if cleaned_sentence:
            yield cleaned_sentence

@stage
def count_words(sentence: str):
    """Takes a sentence and returns a tuple of (word_count, sentence)."""
    word_count = len(sentence.split())
    return (word_count, sentence)

@stage
def to_dict(data: tuple):
    """Takes a tuple and converts it to a dictionary."""
    return {"word_count": data[0], "sentence": data[1]}

def main():
    """Builds and runs the pipeline."""
    # 2. Define the input data
    data = [
        "This is the first sentence. This is the second.",
        "Here is another block of text. It has two more sentences.",
    ]

    # 3. Construct the pipeline by chaining stages together
    pipeline = Pipeline() | split_sentences | count_words | to_dict

    # 4. Run the pipeline and collect the results
    results, context = pipeline.collect(data)

    # 5. Print the results
    print("--- Pipeline Results ---")
    for res in results:
        print(res)

    print("\n--- Final Context ---")
    print(context.to_dict())

if __name__ == "__main__":
    main()
```

Which produces the output:

```
--- Pipeline Results ---
{'word_count': 5, 'sentence': 'This is the first sentence'}
{'word_count': 4, 'sentence': 'This is the second'}
{'word_count': 6, 'sentence': 'Here is another block of text'}
{'word_count': 5, 'sentence': 'It has two more sentences'}

--- Final Context ---
{}
```
