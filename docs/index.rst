Quickstart |:speedboat:|
=======================

The glyphdeck library is a comprehensive toolkit designed to streamline & simplify various aspects of asynchronous LLM data processing workflows over high-volume, dense semantic data - like customer feedback, reviews and comments.

Common aspects of the LLM data workflow are handled end-to-end data validation, sanitisation, transformation, caching and step chaining, facilitating the fast development of robust, error free LLM data workflows. 

Its also equiped with a configurable logging facility that makes complex asyncronous LLM workflows much easier to configure, understand and debug.

Set-up |:wrench:|
----------------------------------

Install |:package:|
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block::
   
   pip install glyphdeck

Set API Key |:clipboard:|
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Set your ``OPENAI_API_KEY`` with a `command <https://platform.openai.com/docs/quickstart?language-preference=python>`_, or add it to your `environment variables <https://en.wikipedia.org/wiki/Environment_variable>`_.

.. code-block:: 
   
   setx OPENAI_API_KEY "your_api_key_here"

Import |:inbox_tray:|
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python
   
   import glyphdeck as gd

Usage |:toolbox:|
----------------------------------

Cascades |:ocean:|
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The Cascade class is the primary interface for the glyphdeck library. It handles and processes data in a record-like structure, providing easy to use syntax for LLM data handling workflows.

It validates and enforces all data movements against a common id, ensuring that each record has a unique, immutable identifier that remains consistent, regardless of other changes.

This all combines to let you spin up LLM workflows in record time.

Example |:man_surfing:|
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude :: _static/index_workflow_example.py
   :language: python3

Output |:beach:|
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+-----------+-----------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------+------------------------------+
| Review Id | Review                                                                                                                                  | Review_sub_categories                                                          | Review_sentiment_score       |
+===========+=========================================================================================================================================+================================================================================+==============================+
| 1         | I love this place, fantastic pizza and great service. It's family run and it shows. Out of heaps of good local options it is still the… | Pizza Quality,Service Quality,Family Run,Local Options,Pricing,Parking         | 0.95                         |
+-----------+-----------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------+------------------------------+
| 2         | Got disappointed. Not good customer service. definitely not great pizza.We went there yesterday to…                                     | Customer Service,Pizza Quality,Dine-In Experience,Promotions,Family Experience | -0.75                        |
+-----------+-----------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------+------------------------------+

.. important:: That's just the start! |:infinity:| Glyphdeck is highly customisable with useful optional commands in every function, class & method. Use the Cascade and other tools to mix and match, create feedback loops |:repeat:| and more - all with blazing |:fire:| asynchronous speed through the OpenAi API.

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