# typed_pipeline_improved.py
"""
A type-aware, composable pipeline framework with:
 1) Unified stage decorator
 2) Extended operator overloading (| and >>)
 3) Lazy/eager execution modes
 4) Streaming with backpressure for thread/process backends
 5) Configurable error-handling (fail/skip/retry)
 6) Thread/Process-safe Context with atomic counters
 7) Richer runtime type checking for typing generics
 8) Pluggable type adapters (coercions)
 9) Branching & merging (simple DAG-style)
10) Metrics & monitoring hooks
11) Checkpointing/resumability (opt-in, simple file-based)
12) Logging/tracing hooks
13) Async/await stage backend using asyncio
14) Serialization of pipeline topology (best-effort)
15) Convenience adapters for pandas (optional) and general mapping ops

Note: This module focuses on core mechanics without external heavy deps.
"""
from __future__ import annotations

import asyncio
import inspect
import json
import os
import pickle
import queue
import threading
import time
import types
import multiprocessing as mp
from dataclasses import dataclass, field
from functools import wraps
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

# -------------------------------------------------
# Utilities
# -------------------------------------------------

SENTINEL = object()


def _ensure_iterable(x: Any) -> Iterable[Any]:
    if x is None:
        return []
    if hasattr(x, "__iter__") and not isinstance(x, (str, bytes, dict)):
        return x
    return [x]


def _is_async_callable(func: Callable[..., Any]) -> bool:
    return asyncio.iscoroutinefunction(func)


# -------------------------------------------------
# Advanced type checking (generics-aware, best-effort)
# -------------------------------------------------


def _check_instance(value: Any, annotation: Any) -> bool:
    """Best-effort runtime check supporting typing generics like List[int].
    Falls back to True if annotation is Any or not enforceable.
    """
    if annotation is Any or annotation is None:
        return True
    origin = get_origin(annotation)
    args = get_args(annotation)

    # Plain class or typing alias without args
    if origin is None:
        try:
            return isinstance(value, annotation)  # type: ignore[arg-type]
        except TypeError:
            return True

    # Handle common containers
    if origin in (list, List):
        if not isinstance(value, list):
            return False
        if not args:
            return True
        return all(_check_instance(v, args[0]) for v in value)
    if origin in (tuple, Tuple):
        if not isinstance(value, tuple):
            return False
        if not args:
            return True
        if len(args) == 2 and args[1] is Ellipsis:
            return all(_check_instance(v, args[0]) for v in value)
        if len(args) != len(value):
            return False
        return all(_check_instance(v, a) for v, a in zip(value, args))
    if origin in (dict, Dict):
        if not isinstance(value, dict):
            return False
        if not args:
            return True
        kt, vt = args
        return all(_check_instance(k, kt) and _check_instance(v, vt) for k, v in value.items())
    if origin in (set, frozenset):
        if not isinstance(value, origin):
            return False
        if not args:
            return True
        return all(_check_instance(v, args[0]) for v in value)

    # Fallback: don't over-enforce
    return True


# -------------------------------------------------
# Type inference helpers
# -------------------------------------------------


def infer_types(func: Callable[..., Any]) -> Tuple[Type[Any], Type[Any]]:
    """Infer input and output types from function type hints.
    Picks first non-`context` parameter as input type; output is return.
    """
    hints = get_type_hints(func)
    sig = inspect.signature(func)
    input_type: Any = Any
    for name, param in sig.parameters.items():
        if name == "context":
            continue
        if param.kind in (param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD):
            input_type = hints.get(name, Any)
            break
    output_type: Any = hints.get("return", Any)
    return input_type, output_type


# -------------------------------------------------
# Context (thread/process safe)
# -------------------------------------------------

class Context:
    """A dict-like context with thread/process safety and atomic counters."""

    def __init__(self, mp_safe: bool = False):
        self._mp_safe = mp_safe
        if mp_safe:
            self._manager = mp.Manager()
            self._data = self._manager.dict()  # type: ignore[attr-defined]
            self._lock = self._manager.RLock()  # type: ignore[attr-defined]
        else:
            self._data: Dict[str, Any] = {}
            self._lock = threading.RLock()

    # dict-ish
    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._data[key] = value

    def update(self, other: Dict[str, Any]) -> None:
        with self._lock:
            for k, v in other.items():
                self._data[k] = v

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._data)

    # helpers
    def inc(self, key: str, amount: int = 1) -> int:
        with self._lock:
            self._data[key] = int(self._data.get(key, 0)) + amount
            return self._data[key]

    def __repr__(self) -> str:
        return f"Context(mp_safe={self._mp_safe}, data={self.to_dict()})"


# -------------------------------------------------
# Error handling configuration
# -------------------------------------------------

@dataclass
class ErrorPolicy:
    mode: str = "fail"  # 'fail' | 'skip' | 'retry'
    retries: int = 0
    backoff: float = 0.0  # seconds linear backoff per attempt


# -------------------------------------------------
# Type adapters (coercions)
# -------------------------------------------------

class TypeAdapters:
    def __init__(self):
        self._adapters: Dict[Tuple[Any, Any], Callable[[Any], Any]] = {}

    def register(self, src: Any, dst: Any, func: Callable[[Any], Any]) -> None:
        self._adapters[(src, dst)] = func

    def adapt(self, value: Any, dst: Any) -> Tuple[bool, Any]:
        # Try exact match first
        key = (type(value), dst)
        if key in self._adapters:
            try:
                return True, self._adapters[key](value)
            except Exception:
                return False, value
        # Try common simple casts
        try:
            if dst in (str, int, float, bool):
                return True, dst(value)
        except Exception:
            pass
        return False, value


DEFAULT_ADAPTERS = TypeAdapters()


# -------------------------------------------------
# Hook system (monitoring / tracing)
# -------------------------------------------------

@dataclass
class Hooks:
    before_stage: Optional[Callable[["Stage", Context, Any], None]] = None
    after_stage: Optional[Callable[["Stage", Context, Any, Any, float], None]] = None
    on_error: Optional[Callable[["Stage", Context, Any, BaseException, int], None]] = None


# -------------------------------------------------
# Stage
# -------------------------------------------------

class Stage:
    """Unified stage supporting sync/async callables, type checking, metrics, and errors."""

    def __init__(
        self,
        func: Callable[[Context, Any], Any],
        *,
        input_type: Optional[Type[Any]] = None,
        output_type: Optional[Type[Any]] = None,
        backend: str = "serial",  # 'serial' | 'thread' | 'process' | 'async'
        workers: int = 1,
        error_policy: Optional[ErrorPolicy] = None,
        adapters: Optional[TypeAdapters] = None,
        name: Optional[str] = None,
        hooks: Optional[Hooks] = None,
    ):
        self.func = func
        self.backend = backend
        self.workers = workers
        inferred_in, inferred_out = infer_types(func)
        self.input_type = input_type or inferred_in
        self.output_type = output_type or inferred_out
        self.error_policy = error_policy or ErrorPolicy()
        self.adapters = adapters or DEFAULT_ADAPTERS
        self.name = name or getattr(func, "__name__", func.__class__.__name__)
        self.hooks = hooks or Hooks()
        self._is_async = _is_async_callable(func)

        # metrics
        self.metrics: Dict[str, Any] = {
            "items_in": 0,
            "items_out": 0,
            "errors": 0,
            "time_total": 0.0,
        }

    # -- execution -----------------------------------------------------------
    def _maybe_coerce(self, value: Any, expected: Any) -> Any:
        ok = _check_instance(value, expected)
        if ok:
            return value
        # Try adaptation
        adapted, newv = self.adapters.adapt(value, expected)
        if adapted and _check_instance(newv, expected):
            return newv
        raise TypeError(
            f"Stage {self.name} expected {expected}, got {type(value)}"
        )

    def _apply(self, context: Context, item: Any) -> Iterable[Any]:
        # type check & adapt input
        if self.input_type is not Any:
            item = self._maybe_coerce(item, self.input_type)

        # hooks
        if self.hooks.before_stage:
            try:
                self.hooks.before_stage(self, context, item)
            except Exception:
                pass

        # invoke func with error policy
        attempts = 0
        start = time.perf_counter()
        while True:
            try:
                if self._is_async:
                    # run async func in current loop
                    result = asyncio.run(self.func(context, item))
                else:
                    result = self.func(context, item)
                break
            except BaseException as e:  # noqa: BLE001
                attempts += 1
                self.metrics["errors"] += 1
                if self.hooks.on_error:
                    try:
                        self.hooks.on_error(self, context, item, e, attempts)
                    except Exception:
                        pass
                if self.error_policy.mode == "retry" and attempts <= self.error_policy.retries:
                    time.sleep(self.error_policy.backoff * attempts)
                    continue
                if self.error_policy.mode == "skip":
                    return []
                # fail
                raise
        elapsed = time.perf_counter() - start

        # normalize iterable
        out_iter = _ensure_iterable(result)

        # validate outputs lazily
        def _gen():
            for res in out_iter:
                if self.output_type is not Any and not _check_instance(res, self.output_type):
                    # attempt adapt
                    adapted, newv = self.adapters.adapt(res, self.output_type)
                    if not adapted or not _check_instance(newv, self.output_type):
                        raise TypeError(
                            f"Stage {self.name} returned {type(res)}, expected {self.output_type}"
                        )
                    res = newv
                yield res

        # after hook (pass an iterator preview via list())
        if self.hooks.after_stage:
            try:
                # don't consume generator; just pass timing
                self.hooks.after_stage(self, context, item, None, elapsed)
            except Exception:
                pass

        self.metrics["time_total"] += elapsed
        return _gen()

    def run(self, context: Context, item: Any) -> Iterable[Any]:
        self.metrics["items_in"] += 1
        for res in self._apply(context, item):
            self.metrics["items_out"] += 1
            yield res

    # -- composition ---------------------------------------------------------
    def __or__(self, other: Union["Stage", "Pipeline"]) -> "Pipeline":
        return Pipeline.from_stages([self]) | other

    def __rshift__(self, other: Union["Stage", "Pipeline"]) -> "Pipeline":
        # same as |, provided for ergonomic alternative
        return Pipeline.from_stages([self]) | other


# Unified decorator (Improvement #1)

def stage(
    _func: Optional[Callable[[Context, Any], Any]] = None,
    *,
    backend: str = "serial",
    workers: int = 1,
    input_type: Optional[Type[Any]] = None,
    output_type: Optional[Type[Any]] = None,
    error_policy: Optional[ErrorPolicy] = None,
    adapters: Optional[TypeAdapters] = None,
    name: Optional[str] = None,
    hooks: Optional[Hooks] = None,
) -> Union[Stage, Callable[[Callable[[Context, Any], Any]], Stage]]:
    def wrapper(func: Callable[[Context, Any], Any]) -> Stage:
        return Stage(
            func,
            input_type=input_type,
            output_type=output_type,
            backend=backend,
            workers=workers,
            error_policy=error_policy,
            adapters=adapters,
            name=name,
            hooks=hooks,
        )

    if _func is not None:
        return wrapper(_func)
    return wrapper


# -------------------------------------------------
# Concurrency runners with streaming + backpressure
# -------------------------------------------------

# NOTE: All runners return a generator to preserve laziness.


def run_stage_serial(stage: Stage, context: Context, iterable: Iterable[Any]) -> Iterator[Any]:
    for item in iterable:
        yield from stage.run(context, item)


def run_stage_thread(stage: Stage, context: Context, iterable: Iterable[Any]) -> Iterator[Any]:
    maxsize = max(1, stage.workers * 2)
    q_in: "queue.Queue[Any]" = queue.Queue(maxsize=maxsize)
    q_out: "queue.Queue[Any]" = queue.Queue(maxsize=maxsize)
    done = threading.Event()

    def feeder():
        for item in iterable:
            q_in.put(item)  # blocks when full (backpressure)
        for _ in range(stage.workers):
            q_in.put(SENTINEL)

    def worker():
        while True:
            item = q_in.get()
            if item is SENTINEL:
                break
            try:
                for res in stage.run(context, item):
                    q_out.put(res)
            finally:
                q_in.task_done()
        q_in.task_done()

    def drainer() -> Generator[Any, None, None]:
        sentinel_count = 0
        while True:
            try:
                obj = q_out.get(timeout=0.1)
                yield obj
                q_out.task_done()
            except queue.Empty:
                if done.is_set() and q_out.empty():
                    break

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(stage.workers)]
    t_feed = threading.Thread(target=feeder, daemon=True)
    for t in threads:
        t.start()
    t_feed.start()

    def finalize():
        q_in.join()
        done.set()
        for t in threads:
            t.join()

    # Use a generator that yields as results arrive, then finalizes
    def gen():
        try:
            for x in drainer():
                yield x
        finally:
            finalize()

    return gen()


def run_stage_process(stage: Stage, context: Context, iterable: Iterable[Any]) -> Iterator[Any]:
    maxsize = max(1, stage.workers * 2)
    q_in: mp.Queue = mp.Queue(maxsize=maxsize)
    q_out: mp.Queue = mp.Queue(maxsize=maxsize)

    # Prepare context proxy (process-safe)
    if not context._mp_safe:
        ctx = Context(mp_safe=True)
        ctx.update(context.to_dict())
    else:
        ctx = context

    def feeder_proc(q: mp.Queue):
        for item in iterable:
            q.put(item)
        for _ in range(stage.workers):
            q.put(SENTINEL)

    def worker_proc(qi: mp.Queue, qo: mp.Queue, pickled_stage: bytes, pickled_ctx: bytes):
        st: Stage = pickle.loads(pickled_stage)
        ctx_local: Context = pickle.loads(pickled_ctx)
        while True:
            item = qi.get()
            if item is SENTINEL:
                break
            for res in st.run(ctx_local, item):
                qo.put(res)
        qo.put(SENTINEL)

    pickled_stage = pickle.dumps(stage)
    pickled_ctx = pickle.dumps(ctx)

    feeder = mp.Process(target=feeder_proc, args=(q_in,), daemon=True)
    workers = [mp.Process(target=worker_proc, args=(q_in, q_out, pickled_stage, pickled_ctx), daemon=True) for _ in range(stage.workers)]

    feeder.start()
    for p in workers:
        p.start()

    def gen():
        finished = 0
        try:
            while finished < stage.workers:
                item = q_out.get()
                if item is SENTINEL:
                    finished += 1
                else:
                    yield item
        finally:
            feeder.join()
            for p in workers:
                p.join()
            # merge context back
            context.update(ctx.to_dict())

    return gen()


async def run_stage_async_asyncio(stage: Stage, context: Context, iterable: Iterable[Any]) -> AsyncIterator[Any]:
    for item in iterable:
        # Run possibly sync func in thread to avoid blocking loop
        if stage._is_async:
            results = await stage.func(context, item)  # type: ignore[misc]
        else:
            results = await asyncio.to_thread(stage.func, context, item)
        for res in _ensure_iterable(results):
            yield res


# registry
STAGE_RUNNERS: Dict[str, Callable[[Stage, Context, Iterable[Any]], Iterator[Any]]] = {
    "serial": run_stage_serial,
    "thread": run_stage_thread,
    "process": run_stage_process,
}


def run_stage(stage: Stage, context: Context, iterable: Iterable[Any]) -> Iterable[Any]:
    if stage.backend == "async":
        # Wrap async into synchronous generator by spinning a private loop
        async def _async_collect():
            out: List[Any] = []
            async for x in run_stage_async_asyncio(stage, context, iterable):
                out.append(x)
            return out

        return (x for x in asyncio.run(_async_collect()))

    runner = STAGE_RUNNERS.get(stage.backend)
    if runner is None:
        raise ValueError(f"Unknown backend {stage.backend}")
    return runner(stage, context, iterable)


# -------------------------------------------------
# Branching & Merging (Improvement #9)
# -------------------------------------------------

class Branch:
    """Fan-out into multiple branches and then merge their outputs per input item.
    Each branch is a Stage or Pipeline that consumes a single item and yields zero or more results.
    The merge strategy controls how branch outputs are combined per input.
    """

    def __init__(self, *branches: Union[Stage, "Pipeline"], merge: str = "zip"):
        self.branches: List[Pipeline] = [Pipeline.from_stages([b]) if isinstance(b, Stage) else b for b in branches]  # type: ignore[arg-type]
        self.merge = merge  # 'zip' | 'concat' | 'tuple_first'

    def to_stage(self) -> Stage:
        def _branch(context: Context, item: Any) -> Iterable[Any]:
            results: List[List[Any]] = []
            for br in self.branches:
                out_iter = br._run_iter([item], context)
                results.append(list(out_iter))
            if self.merge == "concat":
                for lst in results:
                    for x in lst:
                        yield x
            elif self.merge == "tuple_first":
                yield tuple(lst[0] if lst else None for lst in results)
            else:  # zip
                for tup in zip(*results):
                    yield tup
        return Stage(_branch, name="Branch")


# -------------------------------------------------
# Pipeline core
# -------------------------------------------------

class Pipeline:
    def __init__(self, stages: Optional[List[Stage]] = None, *, name: Optional[str] = None):
        self.stages: List[Stage] = stages or []
        self.name = name or "Pipeline"
        self._check_types()
        self._metrics: Dict[str, Any] = {"stages": {}}

    # Factory
    @staticmethod
    def from_stages(stages: List[Stage]) -> "Pipeline":
        return Pipeline(stages)

    # Composition
    def add(self, obj: Union[Stage, "Pipeline"]) -> "Pipeline":
        if isinstance(obj, Pipeline):
            self.stages.extend(obj.stages)
        else:
            self.stages.append(obj)
        self._check_types()
        return self

    def branch(self, *branches: Union[Stage, "Pipeline"], merge: str = "zip") -> "Pipeline":
        return self | Branch(*branches, merge=merge).to_stage()

    def merge(self, func: Callable[[Context, Any], Any]) -> "Pipeline":
        return self | Stage(func)

    # Operators (| and >>)
    def __or__(self, other: Union[Stage, "Pipeline"]) -> "Pipeline":
        if isinstance(other, Pipeline):
            return Pipeline(self.stages + other.stages)
        return Pipeline(self.stages + [other])

    def __rshift__(self, other: Union[Stage, "Pipeline"]) -> "Pipeline":
        return self.__or__(other)

    # Type checks between consecutive stages
    def _check_types(self) -> None:
        for i in range(len(self.stages) - 1):
            prev_out = getattr(self.stages[i], "output_type", Any)
            next_in = getattr(self.stages[i + 1], "input_type", Any)
            if prev_out is not Any and next_in is not Any:
                # best-effort subtype check
                if get_origin(prev_out) is None and get_origin(next_in) is None:
                    try:
                        if not issubclass(prev_out, next_in):
                            raise TypeError(
                                f"Type mismatch: stage {i} output {prev_out}, stage {i+1} input {next_in}"
                            )
                    except TypeError:
                        # Non-class types; skip strict enforcement
                        pass

    # Execution helpers
    def _need_mp(self) -> bool:
        return any(getattr(s, "backend", "serial") == "process" for s in self.stages)

    def _build_context(self) -> Context:
        return Context(mp_safe=self._need_mp())

    def _run_iter(self, data: Iterable[Any], context: Optional[Context] = None) -> Iterable[Any]:
        ctx = context or self._build_context()
        stream: Iterable[Any] = data
        for st in self.stages:
            stream = run_stage(st, ctx, stream)
        return stream

    # Public API: lazy generator by default (Improvement #3)
    def run(self, data: Iterable[Any], *, collect: bool = False, checkpoint_path: Optional[str] = None, resume: bool = False) -> Tuple[Iterable[Any], Context]:
        ctx = self._build_context()
        stream = data

        if checkpoint_path:
            os.makedirs(checkpoint_path, exist_ok=True)

        def with_checkpoint(iterable: Iterable[Any], stage_idx: int) -> Iterable[Any]:
            # Simple per-stage checkpoint: write outputs to files item-by-item
            if not checkpoint_path:
                return iterable

            fn = os.path.join(checkpoint_path, f"stage_{stage_idx}.pkl")
            seen = 0
            if resume and os.path.exists(fn):
                try:
                    with open(fn, "rb") as f:
                        cached: List[Any] = pickle.load(f)
                    seen = len(cached)
                    # yield cached first, then continue
                    for x in cached:
                        yield x
                except Exception:
                    seen = 0

            out: List[Any] = []
            for x in iterable:
                out.append(x)
                yield x
            try:
                with open(fn, "wb") as f:
                    pickle.dump(out, f)
            except Exception:
                pass

        for idx, st in enumerate(self.stages):
            stream = run_stage(st, ctx, stream)
            stream = with_checkpoint(stream, idx)

        return (list(stream) if collect else stream), ctx

    # Convenience eager collection
    def collect(self, data: Iterable[Any]) -> Tuple[List[Any], Context]:
        stream, ctx = self.run(data, collect=True)
        return stream, ctx

    # Metrics
    @property
    def metrics(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"stages": {}}
        for st in self.stages:
            out["stages"][st.name] = dict(st.metrics)
        return out

    # Serialization of topology (Improvement #14)
    def to_spec(self) -> Dict[str, Any]:
        def stage_spec(st: Stage) -> Dict[str, Any]:
            return {
                "name": st.name,
                "backend": st.backend,
                "workers": st.workers,
                "input_type": str(st.input_type),
                "output_type": str(st.output_type),
                "error_policy": st.error_policy.__dict__,
                "is_async": st._is_async,
            }

        return {
            "name": self.name,
            "stages": [stage_spec(s) for s in self.stages],
        }

    @classmethod
    def from_spec(cls, spec: Dict[str, Any], *, funcs: Dict[str, Callable[[Context, Any], Any]]) -> "Pipeline":
        stages: List[Stage] = []
        for s in spec.get("stages", []):
            fname = s["name"]
            func = funcs[fname]
            stages.append(
                Stage(
                    func,
                    backend=s.get("backend", "serial"),
                    workers=int(s.get("workers", 1)),
                    name=fname,
                )
            )
        return Pipeline(stages, name=spec.get("name", "Pipeline"))


# -------------------------------------------------
# Convenience helpers (split, batch, reduce, map, df adapters)
# -------------------------------------------------

# split: fan-out iterable returned by func over each input

def split(func: Callable[[Any], Iterable[Any]] = lambda x: [x]) -> Stage:
    def _stage(context: Context, item: Any) -> Iterable[Any]:
        yield from func(item)
    return Stage(_stage, name="split")


# batch: group items into fixed-size lists and apply func

def batch(size: int = 1, func: Callable[[List[Any]], Any] = lambda x: list(x)) -> Stage:
    assert size >= 1

    def _stage(context: Context, iterable: Iterable[Any]) -> Iterable[Any]:
        buf: List[Any] = []
        for item in iterable:
            buf.append(item)
            if len(buf) >= size:
                yield func(buf)
                buf.clear()
        if buf:
            yield func(buf)

    return Stage(_stage, name=f"batch_{size}")


# reduce: fold over an iterable with an accumulator

def reduce(func: Callable[[Any, Any], Any], initializer: Optional[Any] = None) -> Stage:
    def _stage(context: Context, iterable: Iterable[Any]) -> Iterable[Any]:
        it = iter(iterable)
        if initializer is None:
            try:
                acc = next(it)
            except StopIteration:
                return
        else:
            acc = initializer
        for item in it:
            acc = func(acc, item)
        yield acc

    return Stage(_stage, name="reduce")


# map_values: simple map over items

def map_values(func: Callable[[Any], Any], *, name: Optional[str] = None) -> Stage:
    def _stage(context: Context, item: Any) -> Iterable[Any]:
        yield func(item)
    return Stage(_stage, name=name or getattr(func, "__name__", "map_values"))


# Optional pandas adapters (Improvement #15)
try:
    import pandas as pd  # type: ignore

    def map_df(fn: Callable[["pd.DataFrame"], "pd.DataFrame"]) -> Stage:
        def _stage(context: Context, df: "pd.DataFrame") -> Iterable[Any]:
            out = fn(df)
            yield out
        return Stage(_stage, name=getattr(fn, "__name__", "map_df"))

except Exception:  # pragma: no cover
    pd = None  # type: ignore
    def map_df(fn: Callable[[Any], Any]) -> Stage:  # fallback noop typing
        return map_values(fn, name="map_df")


# -------------------------------------------------
# Example usage & self-test
# -------------------------------------------------
if __name__ == "__main__":
    # Example stages
    @stage
    def split_words(context: Context, text: str) -> Iterable[str]:
        context.inc("split_calls")
        return text.split()

    @stage(output_type=str)
    def to_upper(context: Context, word: str) -> str:
        return word.upper()

    @stage
    def join_words(context: Context, words: List[str]) -> str:
        return " ".join(words)

    # Branching example: upper and identity, then tuple_first merge
    identity = map_values(lambda x: x, name="identity")

    pipeline = Pipeline() | split_words | to_upper | batch(3) | join_words
    results, ctx = pipeline.collect(["hello brave new", "world of pipelines", "is cool"])
    print("Results:", results)
    print("Context:", ctx.to_dict())
    print("Metrics:", json.dumps(pipeline.metrics, indent=2))

    # Streaming run
    stream, ctx2 = pipeline.run(["streaming is nice", "and lazy by default"], collect=False)
    print(list(stream))

    # Serialization (topology only)
    spec = pipeline.to_spec()
    print("Spec:", json.dumps(spec, indent=2))
