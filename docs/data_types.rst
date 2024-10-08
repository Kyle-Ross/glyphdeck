data types
==========================================

DataDict
-------------------------------------------

The `DataDict` type is the required format for operations within the Cascade and other parts of the glyphdeck library.

.. autodata:: glyphdeck.DataDict

A basic dict with a nested list. 

The key can be either an `int` or a `str`, as long as it is unique. This corresponds to the ``id_column`` argument in ``Cascade``.

The list contains the data to be processed, with each item representing the data for that column. This corresponds to the ``data_columns`` argument in ``Cascade``.

.. code-block:: python
    
    example: DataDict = {
        1: ["Sushi", "Japan"],
        2: ["Paella", "Spain"],
        3: ["Pizza", "Italy"],
    }

.. tip:: Use ``glyphdeck.prepare`` return your dataframe as a tuple including itself as a DataDict

.. note:: Cascade handles conversion into a DataDict automatically when you create an instance

.. autofunction:: glyphdeck.prepare

Record types
-------------------------------------------

The record types used to pass through the Cascade.

.. tip:: These are generated inside the Cascade. You can easily access & manipulate the records using its properties and methods.

.. autodata:: glyphdeck.RecordDict

TODO short desc

Example record, dict render?

.. autodata:: glyphdeck.RecordsDict

TODO short desc

Example records, dict render?