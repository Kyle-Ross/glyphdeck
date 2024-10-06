Quickstart
=======================

The glyphdeck library is a comprehensive toolkit designed to streamline & simplify various aspects of asynchronous LLM data processing workflows over high-volume, dense semantic data - like customer feedback, reviews and comments.

Common aspects of the LLM data workflow are handled end-to-end data validation, sanitisation, transformation, caching and step chaining, facilitating the fast development of robust, error free LLM data workflows. 

Its also equiped with a configurable logging facility that makes complex asyncronous LLM workflows much easier to configure, understand and debug.

Installation
----------------------------------

.. code-block::
   
   pip install glyphdeck

.. code-block:: python
   
   import glyphdeck as gd

Cascades |:ocean:|
----------------------------------
The Cascade class is the primary interface for the glyphdeck library. It handles and processes data in a record-like structure, providing easy to use syntax for LLM data handling workflows.

It validates and enforces all data movements against a common id, ensuring that each record has a unique, immutable identifier that remains consistent, regardless of other changes.

This all combines to let you spin up LLM workflows in record time.

.. literalinclude :: _static/index_workflow_example.py
   :language: python3

Output
""""""""

+-----------+-----------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------+------------------------------+
| Review Id | Review Text                                                                                                                             | Review Text_sub_categories                                                     | Review Text_sentiment_score  |
+===========+=========================================================================================================================================+================================================================================+==============================+
| 1         | I love this place, fantastic pizza and great service. It's family run and it shows. Out of heaps of good local options it is still the… | Pizza Quality,Service Quality,Family Run,Local Options,Pricing,Parking         | 0.95                         |
+-----------+-----------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------+------------------------------+
| 2         | Got disappointed. Not good customer service. definitely not great pizza.We went there yesterday to…                                     | Customer Service,Pizza Quality,Dine-In Experience,Promotions,Family Experience | -0.75                        |
+-----------+-----------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------+------------------------------+


.. toctree::
   :maxdepth: 2
   :caption: INTERFACE
   :hidden:

   glyphdeck

.. toctree::
   :maxdepth: 2
   :caption: API
   :hidden:

   glyphdeck.processors
   glyphdeck.validation
   glyphdeck.config
   glyphdeck.tools

.. toctree::
   :maxdepth: 1
   :caption: OTHER
   :hidden:

   gen_index