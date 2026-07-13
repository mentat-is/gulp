"""
pydantic models related utilities
"""

from pydantic import BaseModel


def autogenerate_model_example_by_class(model_class: BaseModel):
    """
    helper method to generate an example for a pydantic model, using the examples from the fields.

    Args:
        model_class (BaseModel): the model to generate the example for.
    """
    example = {}

    # check if model has a json_schema_extra with examples
    if model_class.model_config and "json_schema_extra" in model_class.model_config:
        # take first example if multiple exist
        if "examples" in model_class.model_config["json_schema_extra"]:
            example = model_class.model_config["json_schema_extra"]["examples"][0]
            return example

    # build example from fields (if they have examples)
    for field_name, field in model_class.model_fields.items():
        if field.json_schema_extra and "examples" in field.json_schema_extra:
            # take first example if multiple exist
            example[field_name] = field.json_schema_extra["examples"][0]
        # fallback for legacy
        elif hasattr(field, "example") and field.example:
            example[field_name] = field.example[0]
    return example
