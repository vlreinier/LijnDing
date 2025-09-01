from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Iterable, Iterator

from ..core.utils import ensure_iterable
from .base import BaseRunner

if TYPE_CHECKING:
    from ..core.context import Context
    from ..core.stage import Stage


class SerialRunner(BaseRunner):
    """
    A runner that executes stages sequentially in the main thread.
    """

    def _run_itemwise(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        """
        Processes items one by one in a simple loop.
        """
        for item in iterable:
            stage.metrics["items_in"] += 1
            attempts = 0
            start_time = time.perf_counter()

            if stage.hooks and stage.hooks.before_stage:
                stage.hooks.before_stage(stage, context, item)

            while True:
                try:
                    # Invoke the stage function
                    results = stage._invoke(context, item)

                    # The result of an itemwise stage can be a single item or an iterable
                    output_stream = ensure_iterable(results)

                    count = 0
                    for res in output_stream:
                        stage.metrics["items_out"] += 1
                        count += 1
                        yield res

                    stage.logger.debug(f"Successfully processed item, produced {count} output item(s).")

                    # If successful, break the retry loop
                    break

                except Exception as e:
                    stage.logger.warning(f"Error processing item: {e}", exc_info=True)
                    attempts += 1
                    stage.metrics["errors"] += 1

                    if stage.hooks and stage.hooks.on_error:
                        stage.hooks.on_error(stage, context, item, e, attempts)

                    policy = stage.error_policy
                    if policy.mode == "retry" and attempts <= policy.retries:
                        if policy.backoff > 0:
                            time.sleep(policy.backoff * attempts)
                        continue  # Try again

                    if policy.mode == "skip":
                        break  # Stop processing this item and move to the next

                    # Default mode is 'fail'
                    raise e

                finally:
                    elapsed = time.perf_counter() - start_time
                    stage.metrics["time_total"] += elapsed

                    if stage.hooks and stage.hooks.after_stage:
                        # We pass the original item and a placeholder for the result,
                        # as the result could be a consumed generator.
                        stage.hooks.after_stage(stage, context, item, None, elapsed)
