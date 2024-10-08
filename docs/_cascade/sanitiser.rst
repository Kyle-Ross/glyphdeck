sanitiser
=============================================

The ``sanitiser`` is used to identify and replace pieces of private information
within ``DataDicts`` using regular expression patterns. 

It supports sanitisation of emails, URLs, file paths,
folder paths, dates, numbers and any other regex you want to add in.

Usage
----------------------------------------------

Each ``Cascade`` is initialised with an instance of the ``BaseSanitiser`` class. 

This can be accessed like so:

.. code-block:: python

    cascade = gd.Cascade(...)
    cascade.sanitiser.[...]

.. autoattribute:: glyphdeck.Cascade.sanitiser

Instance Behaviour
----------------------------------------------

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
----------------------------------------------

The ``.sanitiser`` also inherits the features of the ``BaseSanitiser``. 

.. autoclass:: glyphdeck.processors.sanitiser.BaseSanitiser
    :members:
    :exclude-members: patterns, sanitise, values, key

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