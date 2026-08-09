from dataclasses import dataclass

from .entity_key import ENTITY_KEY_SCHEMA

GLOBAL_ENTITY_KEY_SCHEMA = "global-v1"
CUSTOM_ENTITY_KEY_SCHEMA = "custom-v1"


@dataclass(frozen=True, slots=True)
class EntityIdentity:
    schema: str
    key: str


def identify_entity(entity_id: str) -> EntityIdentity:
    if entity_id == "__global":
        return EntityIdentity(GLOBAL_ENTITY_KEY_SCHEMA, entity_id)
    if entity_id.startswith(f"{ENTITY_KEY_SCHEMA}:"):
        return EntityIdentity(ENTITY_KEY_SCHEMA, entity_id)
    return EntityIdentity(CUSTOM_ENTITY_KEY_SCHEMA, entity_id)
