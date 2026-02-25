from enum import StrEnum, auto
from typing import Any
from pydantic import BaseModel
from collections import Counter
from collections.abc import Hashable

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
    def find_conflicts(cls, changes: list[dict[tuple[Hashable, ...], Change]]) -> dict[tuple[Hashable, ...], list[Change]]:
        """
        Finds conflicts in a list of changes.

        Args:
           changes: A list of dictionaries representing changes to a state.

        Returns:
            A dictionary mapping a path in the state as a list of keys to lists of conflicting changes directly under that path.
        """

        if len(changes) <= 1:
            return {}
        
        counts = Counter(key for d in changes for key in d)

        duplicate_keys = [k for k, count in counts.items() if count > 1]

        conflicts: dict[tuple[Hashable, ...], list[Change]] = {}        
        for key in duplicate_keys:
            conflicts[key] = [d[key] for d in changes if key in d]

        return conflicts


    @classmethod
    def recursive_diff(cls, old: Any, new: Any, path: tuple[Hashable, ...] | None = None) -> dict[tuple[Hashable, ...], Change]:
        """
        Recursively computes the differences between two dictionaries.


        Args:
            old: Part of the old dictionary.
            new: Part of the new dictionary.
            path: The current path of the parts in the full dictionary as a list of keys from least to most specific.

        Returns:
            A mapping of the path to the changes directly on that level.
        """

        path = path or ()
        changes: dict[tuple[Hashable, ...], Change] = {}

        if isinstance(old, dict) and isinstance(new, dict):
            all_keys: set[str] = set(old.keys()) | set(new.keys()) #type: ignore

            for key in all_keys:
                current_path: tuple[Hashable, ...] = (*path, key)

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
    def apply_changes(cls, target: dict[Hashable, Any], changes: dict[tuple[Hashable, ...], Change]) -> None:
        """
        Applies a set of changes to the target dictionary.


        Args:
            target: The dictionary to apply the changes to.
            changes: A mapping of paths to changes. The paths are tuples of keys that lead to the value that needs to changes. The changes are applied in the dictionary on that level.
        """

        for path, change in changes.items():
            cursor = target
            
            # Navigate down the dictionary
            for part in path[:-1]:
                if part not in cursor:
                    cursor[part] = {} # If the path was created because of ADDED
                cursor = cursor[part]
            
            last_key = path[-1]

            if change.type == ChangeTypes.REMOVED:
                print("DELETE KEY:")
                print(last_key)
                if last_key in cursor:
                    del cursor[last_key]
                else:
                    raise KeyError(f"Unable to remove key: {last_key} not found in target dictionary under path {path} from {target}")
                
            else:
                # UPDATED or ADDED
                cursor[last_key] = change.new



class ChangeConflictException(Exception):
    """
    Exception raised when a conflict between changes to a state is detected.
    """
    pass