Backends API
============

This page contains the API for the execution backends.

Runner Registry
---------------
.. autofunction:: lijnding.backends.get_runner

Base Runner
-----------
.. autoclass:: lijnding.backends.BaseRunner
   :members:
   :undoc-members:
   :show-inheritance:

Concrete Runners
----------------
.. autoclass:: lijnding.backends.SerialRunner
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: lijnding.backends.ThreadingRunner
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: lijnding.backends.ProcessingRunner
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: lijnding.backends.AsyncioRunner
   :members:
   :undoc-members:
   :show-inheritance:
