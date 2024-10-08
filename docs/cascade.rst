Cascade
==============================

About
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``Cascade`` class is the primary interface for the glyphdeck library. It handles and processes data in a record-like structure, providing easy to use syntax for LLM data handling workflows.

It validates and enforces all data movements against a common id, ensuring that each record has a unique, immutable identifier that remains consistent, regardless of other changes.

Inherits the functionalities of other modules across the library for seemless use, including the ``BaseSanitiser`` & ``BaseLLMHandler``.

Example
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude :: _static/index_workflow_example.py
   :language: python3


Methods & Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: glyphdeck.Cascade


.sanitiser
---------------------------------------------

TODO running sanitiser inside cascade, link to BaseSanitiser notes

.llm_handler
---------------------------------------------