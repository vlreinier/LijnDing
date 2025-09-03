from __future__ import annotations

from typing import Any, Iterable, List

from ..core.stage import Stage, aggregator_stage


def batch(size: int = 10) -> Stage:
    """
    Creates a stage that groups items from an input stream into batches (lists).

    This is an aggregator stage. It consumes the entire input iterable and
    yields a new iterable of lists, where each list is a batch of items.

    Args:
        size: The desired number of items per batch.

    Returns:
        A Stage configured to perform batching.
    """
    if not isinstance(size, int) or size <= 0:
        raise ValueError("Batch size must be a positive integer.")

    @aggregator_stage(name=f"batch(size={size})")
    def _batch_func(iterable: Iterable[Any]) -> Iterable[List[Any]]:
        batch: List[Any] = []
        for item in iterable:
            batch.append(item)
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch

    return _batch_func
