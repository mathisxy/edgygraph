from enum import StrEnum, auto
from typing import Any
from pydantic import BaseModel
from collections import Counter

from .rich import RichReprMixin


class ChangeTypes(StrEnum):
    """
    Enum for the types of changes that can be made to a State.
    """

    ADDED = auto()
    REMOVED = auto()
    UPDATED = auto()

class Change(RichReprMixin, BaseModel):
    """
    Represents a change made to a State.
    """

    type: ChangeTypes
    old: Any
    new: Any



class Diff:
    """
    Utility class for computing differences between states.
    """


    @classmethod
    def find_conflicts(cls, changes: list[dict[str, Change]]) -> dict[str, list[Change]]:
        """
        Finds conflicts in a list of changes.

        Args:
           changes: A list of dictionaries representing changes to a state.

        Returns:
            A dictionary mapping keys to lists of conflicting changes.
        """

        if len(changes) <= 1:
            return {}
        
        counts = Counter(key for d in changes for key in d)

        duplicate_keys = [k for k, count in counts.items() if count > 1]

        conflicts: dict[str, list[Change]] = {}        
        for key in duplicate_keys:
            conflicts[key] = [d[key] for d in changes if key in d]

        return conflicts


    @classmethod
    def recursive_diff(cls, old: Any, new: Any, path: str = "") -> dict[str, Change]:
        """
        Recursively computes the differences between two dictionaries.


        Args:
            old: Part of the old dictionary.
            new: Part of the new dictionary.
            path: The current path of the parts in the full dictionary, seperated with dots.

        Returns:
            A mapping of the path to the changes directly on that level.
        """
        
        changes: dict[str, Change] = {}

        if isinstance(old, dict) and isinstance(new, dict):
            all_keys: set[str] = set(old.keys()) | set(new.keys()) #type: ignore

            for key in all_keys:
                current_path: str = f"{path}.{key}" if path else key

                if key in old and not key in new:
                    changes[current_path] = Change(type=ChangeTypes.REMOVED, old=old[key], new=None)
                elif key in new and not key in old:
                    changes[current_path] = Change(type=ChangeTypes.ADDED, old=None, new=new[key])
                else:
                    sub_changes = cls.recursive_diff(old[key], new[key], current_path)
                    changes.update(sub_changes)

        elif old != new:
            changes[path] = Change(type=ChangeTypes.UPDATED, old=old, new=new)

        return changes
    

    @classmethod
    def apply_changes(cls, target: dict[str, Any], changes: dict[str, Change]) -> None:
        """
        Applies a set of changes to the target dictionary.


        Args:
            target: The dictionary to apply the changes to.
            changes: A mapping of paths, separated by dots, to changes. The changes are applied in the dictionary on that level.
        """

        for path, change in changes.items():
            parts = path.split(".")
            cursor = target
            
            # Navigate down the dictionary
            for part in parts[:-1]:
                if part not in cursor:
                    cursor[part] = {} # If the path was created because of ADDED
                cursor = cursor[part]
            
            last_key = parts[-1]

            if change.type == ChangeTypes.REMOVED:
                if last_key in cursor:
                    del cursor[last_key]
            else:
                # UPDATED or ADDED
                cursor[last_key] = change.new



class ChangeConflictException(Exception):
    """
    Exception raised when a conflict between changes to a state is detected.
    """
    pass