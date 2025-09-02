from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Iterable, Iterator

from ..core.utils import ensure_iterable
from .base import BaseRunner, _handle_error_routing

if TYPE_CHECKING:
    from ..core.context import Context
    from ..core.stage import Stage


class SerialRunner(BaseRunner):
    """
    A runner that executes stages sequentially in the main thread.
    """

    def _run_itemwise(self, stage: "Stage", context: "Context", iterable: Iterable[Any]) -> Iterator[Any]:
        """
        Processes items one by one in a simple loop, with structured logging.
        """
        stage.logger.info("stream_started")
        total_items_in = 0
        total_items_out = 0
        stream_start_time = time.perf_counter()

        try:
            for item in iterable:
                total_items_in += 1
                stage.metrics["items_in"] += 1
                item_start_time = time.perf_counter()
                attempts = 0

                if stage.hooks and stage.hooks.before_stage:
                    stage.hooks.before_stage(stage, context, item)

                while True:
                    try:
                        results = stage._invoke(context, item)
                        output_stream = ensure_iterable(results)

                        count_out = 0
                        for res in output_stream:
                            stage.metrics["items_out"] += 1
                            total_items_out += 1
                            count_out += 1
                            yield res

                        item_elapsed = time.perf_counter() - item_start_time
                        stage.logger.debug(
                            "item_processed",
                            item_in=total_items_in,
                            items_out=count_out,
                            duration=round(item_elapsed, 4),
                        )
                        break  # Success, exit retry loop

                    except Exception as e:
                        attempts += 1
                        stage.metrics["errors"] += 1
                        item_elapsed = time.perf_counter() - item_start_time
                        stage.logger.warning(
                            "item_error",
                            item_in=total_items_in,
                            error=str(e),
                            attempts=attempts,
                            duration=round(item_elapsed, 4),
                        )

                        if stage.hooks and stage.hooks.on_error:
                            stage.hooks.on_error(stage, context, item, e, attempts)

                        policy = stage.error_policy
                        if policy.mode == "route_to_stage":
                            _handle_error_routing(stage, context, item)
                            break
                        if policy.mode == "retry" and attempts <= policy.retries:
                            if policy.backoff > 0:
                                time.sleep(policy.backoff * attempts)
                            continue
                        if policy.mode == "skip":
                            break
                        raise e

                    finally:
                        elapsed = time.perf_counter() - item_start_time
                        stage.metrics["time_total"] += elapsed
                        if stage.hooks and stage.hooks.after_stage:
                            stage.hooks.after_stage(stage, context, item, None, elapsed)
        finally:
            total_duration = time.perf_counter() - stream_start_time
            stage.logger.info(
                "stream_finished",
                items_in=total_items_in,
                items_out=total_items_out,
                errors=stage.metrics["errors"],
                duration=round(total_duration, 4),
            )
