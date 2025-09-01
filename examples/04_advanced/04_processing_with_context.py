"""
An example showing how to use a shared Context with the 'process' backend.
"""
from lijnding import stage, Context, Pipeline

# NOTE: The 'process' backend is currently unstable and disabled in this version.
# This example shows how it *would* be used if it were functional.

@stage(backend="process", workers=2)
def process_and_count(context: Context, item: dict):
    """
    Processes an item and updates a shared counter in the context.
    """
    total_processed = context.inc("total_counter")
    if item["type"] == "widget":
        context.inc("widget_counter")
    return item["value"]

def main():
    data = [
        {"id": 1, "type": "widget", "value": 10},
        {"id": 2, "type": "gadget", "value": 20},
        {"id": 3, "type": "widget", "value": 30},
    ]

    pipeline = Pipeline([process_and_count])

    print("--- Attempting to run pipeline with 'process' backend and context ---")
    try:
        results, final_context = pipeline.collect(data)
    except NotImplementedError as e:
        print(f"\nCaught expected error: {e}")
        print("The 'process' backend is disabled in this version of LijnDing.")

if __name__ == "__main__":
    main()
