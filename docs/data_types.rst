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
        1: ["Delicious and fresh", "Rich culture"],
        2: ["Oversalted and soggy", "Warm but crowded"],
        3: ["Comforting and cheesy", "Historical beauty"],
    }

.. tip:: Use ``glyphdeck.prepare`` return your dataframe as a tuple including itself as a DataDict

.. note:: Cascade handles conversion into a DataDict automatically when you create an instance

.. autofunction:: glyphdeck.prepare

Record types
-------------------------------------------

The record types used to pass through the Cascade.

.. tip:: These are generated inside the Cascade. You can easily access & manipulate the records using its properties and methods.

.. autodata:: glyphdeck.RecordDict

Metadata for data entries in the Cascade. One is recorded each time new or transformed data is appended into a Cascade instance.

The ``data`` corresponds to the ``DataDict`` type.

.. code-block:: python

    {
        "title": "Reviews",
        "dt": datetime.datetime(2024, 10, 8, 17, 45, 2, 285588),
        "delta": datetime.timedelta(0),
        "data": {
            1: ["Delicious and fresh", "Rich culture"],
            2: ["Oversalted and soggy", "Warm but crowded"],
            3: ["Comforting and cheesy", "Historical beauty"],
        },
        "column_names": [
            "Food Review", 
            "Country Review"
        ],
    }

.. autodata:: glyphdeck.RecordsDict

Stores multiple individual records in order of addition, making them easily available for access via the properties and methods of the ``Cascade`` class.

Each individual record only contains the current version of the data.

For example, this would be the records (per data in the DataDicts example) when a ``Sentiment`` validator was run on it:

.. code-block:: python

    {
        0: { ... },
        1: { ... },
        2: {
            "title": "LLM Sentiment",
            "dt": datetime.datetime(2024, 10, 8, 17, 59, 28, 207103),
            "delta": datetime.timedelta(microseconds=218445),
            "data": {
                1: [0.8, 0.8], 
                2: [-0.75, 0.2], 
                3: [0.75, 0.5]
            },
            "column_names": [
                "Food Review_sentiment_score", 
                "Country Review_sentiment_score"
            ],
        },
    }


Example records, dict render?