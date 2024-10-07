|:speedboat:| Quickstart
=================================

The glyphdeck library is a comprehensive toolkit designed to streamline & simplify various aspects of asynchronous LLM data processing workflows over high-volume, dense semantic data - like customer feedback, reviews and comments.

Common aspects of the LLM data workflow are handled end-to-end including async llm calls, data validation, sanitisation, transformation, caching and step chaining, facilitating the fast development of robust, error free LLM data workflows. 

Its also equiped with caching and a configurable logging facility that makes complex asyncronous LLM workflows much easier to configure, understand and debug.

|:mag_right:| Features
----------------------------------

- **Asynchronous**: Rip through tabular data with asynchronous llm usage
- **Auto-scaling**: Don't worry about api errors - the ``LLMHandler`` automatically scales to your api limits
- **Rapid config**: Simple syntax in a highly abstracted interface makes set up a breeze
- **Data chaining**: Automatically pass data between steps with the ``Cascade`` class
- **Self validating**: The Cascade class automatically maintains dataset integrity 
- **Pydantic models**: Use built-in or custom Pydantic data models for structured llm output
- **Type enforcement**: Get your structured data in the correct format from the llm every time
- **Private data**: ``Sanitiser`` class strips out common private data patterns before making api calls
- **Automatic caching**: Save on API costs and re-use results
- **Logging**: Built-in logging facility helps to debug your set-ups & peer inside the event loop

|:wrench:| Set-up 
----------------------------------

|:package:| Install
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block::
   
   pip install glyphdeck

|:clipboard:| Set API Key
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Set your ``OPENAI_API_KEY`` with a `command <https://platform.openai.com/docs/quickstart?language-preference=python>`_, or add it to your `environment variables <https://en.wikipedia.org/wiki/Environment_variable>`_.

.. code-block:: 
   
   setx OPENAI_API_KEY "your_api_key_here"

|:inbox_tray:| Import 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python
   
   import glyphdeck as gd

|:toolbox:| Usage
----------------------------------

|:ocean:| Cascades
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The ``Cascade`` class is the primary interface for the glyphdeck library. It handles and processes data in a record-like structure, providing easy to use syntax for LLM data handling workflows.

It validates and enforces all data movements against a common id, ensuring that each record has a unique, immutable identifier that remains consistent, regardless of other changes.

This all combines to let you spin up LLM workflows in record time.

|:man_surfing:| Example
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude :: _static/index_workflow_example.py
   :language: python3

|:beach:| Output
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+-----------+-----------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------+------------------------------+
| Review Id | Review                                                                                                                                  | Review_sub_categories                                                          | Review_sentiment_score       |
+===========+=========================================================================================================================================+================================================================================+==============================+
| 1         | I love this place, fantastic pizza and great service. It's family run and it shows. Out of heaps of good local options it is still the… | Pizza Quality,Service Quality,Family Run,Local Options,Pricing,Parking         | 0.95                         |
+-----------+-----------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------+------------------------------+
| 2         | Got disappointed. Not good customer service. definitely not great pizza.We went there yesterday to…                                     | Customer Service,Pizza Quality,Dine-In Experience,Promotions,Family Experience | -0.75                        |
+-----------+-----------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------+------------------------------+

----------------------------------

.. tip:: That's just the start!

Glyphdeck is highly customisable |:infinity:|  with useful optional commands in every function, 
class & method. Use the Cascade and other tools to mix and match, 
create feedback loops |:repeat:| and more - all with blazing |:fire:| asynchronous speed through the OpenAI API.

.. toctree::
   :maxdepth: 3
   :caption: INTERFACE API
   :hidden:

   glyphdeck

.. toctree::
   :maxdepth: 2
   :caption: COMPONENT API
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