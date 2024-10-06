from docutils import nodes
from sphinx.ext.autodoc import ClassDocumenter, FunctionDocumenter, MethodDocumenter

# Guide:
# If your member name was "Jeff"

# "Documentation for Module: {}"
# becomes...
# "Documentation for Module: Jeff"

# "{}"
# becomes...
# "Jeff"

# Via the .format(name) in each of the conditions in add_header

# Changeable header values and toggle switches
MODULE_HEADER_ENABLED = False  # Set to False to disable module headers
MODULE_HEADER_TEMPLATE = "{}"

CLASS_HEADER_ENABLED = False  # Set to False to disable class headers
CLASS_HEADER_TEMPLATE = "{}"

METHOD_HEADER_ENABLED = True  # Set to False to disable method headers
METHOD_HEADER_TEMPLATE = "{}"

FUNCTION_HEADER_ENABLED = True  # Set to False to disable function headers
FUNCTION_HEADER_TEMPLATE = "{}"

PROPERTY_HEADER_ENABLED = False  # Set to False to disable property headers
PROPERTY_HEADER_TEMPLATE = "{}"

VARIABLE_HEADER_ENABLED = False  # Set to False to disable variable headers
VARIABLE_HEADER_TEMPLATE = "{}"


def setup(app):
    app.add_node(add_header)
    app.connect("autodoc-process-signature", add_header)


class add_header(nodes.General, nodes.Element):
    pass


def add_header(app, what, name, obj, options, signature, return_annotation):
    if what == "module" and MODULE_HEADER_ENABLED:
        header_html = app.builder.templates.render(
            "add_header.html",
            {"title": name, "header": MODULE_HEADER_TEMPLATE.format(name)},
        )
        header_node = add_header(header_html)
        return [header_node]

    elif what == "class" and CLASS_HEADER_ENABLED:
        header_html = app.builder.templates.render(
            "add_header.html",
            {"title": name, "header": CLASS_HEADER_TEMPLATE.format(name)},
        )
        header_node = add_header(header_html)
        return [header_node]

    elif what == "method" and METHOD_HEADER_ENABLED:
        header_html = app.builder.templates.render(
            "add_header.html",
            {"title": name, "header": METHOD_HEADER_TEMPLATE.format(name)},
        )
        header_node = add_header(header_html)
        return [header_node]

    elif what == "function" and FUNCTION_HEADER_ENABLED:
        header_html = app.builder.templates.render(
            "add_header.html",
            {"title": name, "header": FUNCTION_HEADER_TEMPLATE.format(name)},
        )
        header_node = add_header(header_html)
        return [header_node]

    elif what == "property" and PROPERTY_HEADER_ENABLED:
        header_html = app.builder.templates.render(
            "add_header.html",
            {"title": name, "header": PROPERTY_HEADER_TEMPLATE.format(name)},
        )
        header_node = add_header(header_html)
        return [header_node]

    elif what == "data" and VARIABLE_HEADER_ENABLED:  # For module-level variables
        header_html = app.builder.templates.render(
            "add_header.html",
            {"title": name, "header": VARIABLE_HEADER_TEMPLATE.format(name)},
        )
        header_node = add_header(header_html)
        return [header_node]

    return []  # No header for other member types
