"""
An advanced example demonstrating logging, checkpointing, and the use of
new components like filter, read_from_file, and save_progress.
"""
import logging
import os
import time

from lijnding.core import stage
from lijnding.components import filter_, read_from_file, save_progress, write_to_file

# --- Setup ---
# You can configure the logger for LijnDing just like any other Python library.
# Here, we set the level to INFO to see the pipeline and stage lifecycle messages.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CHECKPOINT_FILE = "checkpoint.tmp"
OUTPUT_FILE = "output.tmp"


# --- Stage Definitions ---
@stage
def process_data(context, data: str):
    """A mock processing stage that accesses the logger from the context."""
    context.logger.info(f"Processing item: '{data}'")
    # Simulate some work
    time.sleep(0.1)
    return data.upper()


def main():
    """
    Demonstrates a pipeline with logging, checkpointing, and resumption.
    """
    # Clean up previous run files
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    # --- Part 1: Initial Pipeline Run (simulating a failure) ---
    logger.info("--- Starting Initial Run (will be interrupted) ---")

    # A simple source of data
    initial_data = [f"item_{i}" for i in range(10)]

    # This pipeline will process some data, filter out items containing '3',
    # and save its progress to a checkpoint file.
    pipeline = (
        process_data
        | filter_(lambda x: "3" not in x)
        | save_progress(CHECKPOINT_FILE)
        | write_to_file(OUTPUT_FILE)
    )

    # We'll simulate a failure by only processing the first 5 items.
    # In a real scenario, this could be a crash or a network error.
    try:
        # We run with collect=False because write_to_file is a terminal stage
        # and we don't expect any results back.
        pipeline.run(initial_data[:5])
        logger.info("Initial run 'failed' after processing 5 items.")
    except Exception as e:
        logger.error(f"Pipeline failed unexpectedly: {e}")


    # --- Part 2: Resuming from Checkpoint ---
    logger.info("\n--- Resuming Pipeline from Checkpoint ---")

    # Now, we create a new pipeline that reads from the checkpoint file.
    # This allows us to skip the work that was already completed.
    resume_pipeline = (
        read_from_file(CHECKPOINT_FILE)
        | process_data # We can re-apply processing if needed, or just save
        | write_to_file(OUTPUT_FILE)
    )

    # We run the resume pipeline with the rest of the original data.
    # Note: In a real-world scenario, you might have a more sophisticated
    # way to know what data still needs to be processed. For this example,
    # we just feed the rest of the data to a similar pipeline.
    # A better approach for resumption is to have the source itself be
    # checkpointable.

    # A more realistic resumption: the source is the checkpoint file itself.
    # We build a new pipeline that starts from the checkpoint.

    # Let's check the content of the checkpoint file
    with open(CHECKPOINT_FILE, 'r') as f:
        logger.info(f"Checkpoint file contains: {[line.strip() for line in f.readlines()]}")

    # The `write_to_file` component will overwrite the output file.
    # To resume properly, we should append. Let's make a new writer.

    append_pipeline = (
        read_from_file(CHECKPOINT_FILE) # Start from what we saved
        # The data is already processed and filtered, so we can go straight to the sink.
        # If more processing was needed, it would go here.
    )

    # Let's imagine the original pipeline was just to get the data.
    # Now we want to process it.

    final_pipeline = (
        read_from_file(CHECKPOINT_FILE)
        | process_data
        | write_to_file(OUTPUT_FILE) # This will write the final state
    )

    logger.info("\n--- Running Final Pipeline from Checkpoint Data ---")
    final_pipeline.run([]) # Source stage ignores input, so empty list is fine

    # --- Verification ---
    with open(OUTPUT_FILE, 'r') as f:
        final_content = [line.strip() for line in f.readlines()]
        logger.info(f"\nFinal output file content: {final_content}")
        # Expected: ITEM_0, ITEM_1, ITEM_2, ITEM_4 (item_3 was filtered out)
        assert len(final_content) == 4

    logger.info("\nExample finished successfully!")


if __name__ == "__main__":
    main()
