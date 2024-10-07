logs
=============================

The logging facility can be configured separately against different parts of the library.

Logs can be configured separately between the console and the optional file output, 
which will be created in the ``/output`` folder in the glyphdeck root directory.

.. rubric:: Levels
    :heading-level: 2

Each level is controlled by an `integer` corresponding to those in the core 
python logging library.

You only get logs at or above the level you set.

- 10 - **DEBUG** - all logs (this can be a lot!)
- 20 - **INFO** - summarised information only
- 20 - **ERROR** - errors handled by the library
- 50 - **CRITICAL** - all other errors

.. autofunction:: glyphdeck.reset_logging

    .. note:: 
        - Enables info level logs for all console loggers
        - Disables all file loggers
        - Disables all input/output logging

.. autofunction:: glyphdeck.configure_logging
