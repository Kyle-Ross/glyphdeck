llm_handler
=============================================

About
----------------------------------------------

The ``llm_handler`` is a utility for interacting with Large Language Models (LLMs) within the ``Cascade``.

Usage
----------------------------------------------

Each ``Cascade`` can create an associated ``llm_handler`` instance:

.. code-block:: python

    cascade = gd.Cascade(...)
    cascade.set_llm_handler(...)

Once ``set_llm_handler()`` has been run the ``llm_handler`` can be accessed and ran on the data in the cascade.

.. code-block:: python

    cascade.llm_handler.[...]

.. attribute:: llm_handler

   llm_handler description

   :type: llm_handler

Cascade Extension
----------------------------------------------

The ``.llm_handler`` `instance` available in the ``Cascade`` has some extra 
functionality.

Internally, this is added by the ``CascadeLLMHandler`` super class which adds cascade functionality 
to the base class.

.. autoclass:: glyphdeck.processors.cascade.Cascade.CascadeLLMHandler
    :members: 
    :exclude-members: outer_cascade

BaseLLMHandler
----------------------------------------------

The ``.llm_handler`` inherits the features of the ``BaseLLMHandler``. 

.. autoclass:: glyphdeck.processors.llm_handler.BaseLLMHandler
    :members:
    :exclude-members: _raw_output_data, new_output_data, run_async