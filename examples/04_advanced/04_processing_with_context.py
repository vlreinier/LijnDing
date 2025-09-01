"""
An example showing how to use a shared Context with the 'process' backend.
"""
from lijnding import stage, Context, Pipeline

# This function must be defined at the top-level of a module for some
# serializers to work correctly. `dill` can often handle nested functions,
# but top-level is safest.
@stage(backend="process", workers=2)
def process_and_count(context: Context, item: dict):
    """
    Processes an item and updates a shared counter in the context.
    """
    total_processed = context.inc("total_counter")
    if item["type"] == "widget":
        context.inc("widget_counter")
    print(f"Processing item {item['id']}. Total processed so far: {total_processed}")
    return item["value"]

def main():
    data = [
        {"id": 1, "type": "widget", "value": 10},
        {"id": 2, "type": "gadget", "value": 20},
        {"id": 3, "type": "widget", "value": 30},
        {"id": 4, "type": "widget", "value": 40},
    ]

    pipeline = Pipeline([process_and_count])

    # When a 'process' backend is used, the pipeline automatically creates
    # a process-safe context object.
    results, final_context = pipeline.collect(data)

    print("\n--- Results ---")
    print(f"Sum of values: {sum(results)}")

    print("\n--- Final Context ---")
    # The final context reflects the updates from all worker processes.
    print(final_context.to_dict())
    assert final_context.get("total_counter") == 4
    assert final_context.get("widget_counter") == 3

if __name__ == "__main__":
    # The `if __name__ == "__main__":` guard is crucial for
    # multiprocessing to work correctly on all platforms.
    main()
