data types
==========================================

DataDicts
-------------------------------------------

The common DataDict types which are used across the library.

.. tip:: Use ``glyphdeck.prepare`` return your dataframe as a tuple including itself as a DataDict

.. note:: Cascade does this automatically when you create an instance

.. autodata:: glyphdeck.DataDict

TODO short desc

.. autodata:: glyphdeck.Optional_DataDict

Same as ``DataDict``, but is allowed to be ``None``

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