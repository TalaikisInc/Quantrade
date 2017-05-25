from django import template
from django.forms.forms import BoundField

register = template.Library()


"""
The sole purpose of these filters are to allow operations with template elements
that naturally start with the underscore ('_') character.
"""

@register.filter(name="field_")
def field_(self, name):
    """
    Get a form field starting with _.
    Taken near directly from Djano > forms.
    Returns a BoundField with the given name.
    """
    try:
        field = self.fields[name]
    except KeyError:
        raise KeyError(
            "Key %r not found in '%s'" % (name, self.__class__.__name__))
    return BoundField(self, field, name)


@register.filter(name="attr_")
def attr_(self, name):
    """
    Get an element attribute starting with _.
    Concept from MatZeg on StackOverflow.
    http://stackoverflow.com/users/4026132/matzeg
    """
    try:
        attribute = getattr(self, name)
    except KeyError:
        raise KeyError(
            "Attribute %r not found in '%s'" % (name, self.__class__.__name__))
    return attribute


@register.filter(name="dict_")
def dict_(self, name):
    """
    Mimic a dict get() method startign with _.
    Concept from Timmy O'Mahony on StackOverflow.
    http://stackoverflow.com/users/396300/timmy-omahony
    """
    try:
        dict_element = self.get(name, None)
    except KeyError:
        raise KeyError(
            "Element %r not found in '%s'" % (name, self.__class__.__name__))
    return dict_element


@register.filter(name="multiply")
def multiply(value, arg):
    return float(value)*float(arg)
