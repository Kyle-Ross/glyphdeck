Cascade
=============================================

About
----------------------------------------------

The ``Cascade`` class is the primary interface for the glyphdeck library. It handles and processes data in a record-like structure, providing easy to use syntax for LLM data handling workflows.

It validates and enforces all data movements against a common id, ensuring that each record has a unique, immutable identifier that remains consistent, regardless of other changes.


Inherited Class Instances
----------------------------------------------

The ``Cascade`` is integrated with instances of utility classes:

- :doc:`sanitiser` - Identify and replace pieces of private information within ``DataDicts`` using regular expression patterns.
- :doc:`llm_handler` - Handler for interacting with Large Language Models (LLMs) within the ``Cascade``.


Example
----------------------------------------------

.. literalinclude :: ../_static/index_workflow_example.py
   :language: python3


Methods & Properties
----------------------------------------------

.. autoclass:: glyphdeck.Cascade
    :members:
    :exclude-members: sanitiser, get_combined, 
                      get_rebase, create_dataframes, 
                      expected_len, llm_handler
