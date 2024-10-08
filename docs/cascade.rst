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
    :members:
    :exclude-members: sanitiser, get_combined, 
                      get_rebase, create_dataframes, 
                      expected_len

Inherited Classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

sanitiser
---------------------------------------------

.. autoattribute:: glyphdeck.Cascade.sanitiser

.. autoclass:: glyphdeck.processors.sanitiser.BaseSanitiser
    :members:
    :exclude-members: patterns

.. autoattribute:: glyphdeck.processors.sanitiser.BaseSanitiser.patterns
    :no-value:

    .. note:: 
        >>> # Stores patterns, placeholders & groupings used to sanitise data
        >>> # Adding patterns with Sanitiser methods will insert them here
        >>> {
        >>>     {
        >>>         "group": "date",
        >>>         "placeholder": "<DATE>",
        >>>         "rank": 1,
        >>>         "pattern": _date_pattern1,
        >>>     },
        >>>     ...
        >>> }


llm_handler
---------------------------------------------

:class: Jeff

.. .. autoclass:: glyphdeck.processors.llm_handler.BaseLLMHandler

    