"""Provides a collection of processors designed to manage, process, and sanitize data records.

This package is organized into several modules, each offering distinct functionalities for handling various data processing tasks such as sanitization, handling data with large language models, and cascading data operations.

Modules
-------
**cascade**
    Offers the `Cascade` class to manage and process a sequence of data records with operations such as sanitization and data handling with LLMs.

    This class inherits the functionality of both sanitiser and llm_handler, which can be used within the Cascade instance.

**llm_handler**
    Provides the `LLMHandler` class, for interacting with Large Language Models (LLMs), supporting asynchronous processing, caching, and output management.

**sanitiser**
    Provides the `Sanitiser` class, to sanitize strings by replacing private information with placeholders using regex patterns.
"""
