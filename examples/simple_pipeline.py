"""
A simple example demonstrating the basic usage of the LijnDing framework.
"""
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
