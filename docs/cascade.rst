Cascade
=============================================

Class
---------------------------------------------

About
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``Cascade`` class is the primary interface for the glyphdeck library. It handles and processes data in a record-like structure, providing easy to use syntax for LLM data handling workflows.

It validates and enforces all data movements against a common id, ensuring that each record has a unique, immutable identifier that remains consistent, regardless of other changes.

Inherits the functionalities of other modules across the library for seemless use, including the ``BaseSanitiser`` & ``BaseLLMHandler``.

Example
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude :: _static/index_workflow_example.py
   :language: python3


Methods & Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: glyphdeck.Cascade
    :members:
    :exclude-members: sanitiser, get_combined, 
                      get_rebase, create_dataframes, 
                      expected_len

Inherited Class Instances
---------------------------------------------

The ``Cascade`` is built with easily accessible instances of utility classes.

Sanitiser
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Usage
"""""""""""""""""""""""""""""""""""""""""""""

Each ``Cascade`` is initialised with an instance of the ``BaseSanitiser`` class. 

This can be accessed like so:

.. code-block:: python

    cascade = gd.Cascade(...)
    cascade.sanitiser.[...]

.. autoattribute:: glyphdeck.Cascade.sanitiser

Instance Behaviour
"""""""""""""""""""""""""""""""""""""""""""""

The ``.sanitiser`` available in the ``Cascade`` instance has some extra functionality.

.. attribute:: defaults

   Uses the ``DataDict`` from the latest record by default.

.. attribute:: selected_data

   Data to use instead of the default. Only used when ``use_selected`` is True.

   :type: DataDict

.. attribute:: use_selected

   Whether to use selected data or not.

   :type: bool

.. method:: run(title: str = "sanitised")

   Run the sanitiser and append the result to the cascade.

   :param title: The title to be given to the sanitised record. Defaults to "sanitised".
   :type title: str

   :returns: The sanitiser instance, capable of being further used to cascade additional operations.
   :rtype: sanitiser

   :raises AssertionError: If the provided title argument is not a string.

BaseSanitiser
"""""""""""""""""""""""""""""""""""""""""""""

The ``.sanitiser`` also inherits the features of the ``BaseSanitiser``. 

.. autoclass:: glyphdeck.processors.sanitiser.BaseSanitiser
    :members:
    :exclude-members: patterns, sanitise, values

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
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:class: Jeff

.. .. autoclass:: glyphdeck.processors.llm_handler.BaseLLMHandler

    