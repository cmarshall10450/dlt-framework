"""Decorator registry system for the DLT Medallion Framework."""

from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar, Union

from .exceptions import DecoratorError

F = TypeVar("F", bound=Callable[..., Any])


class DecoratorRegistry:
    """Central registry for managing decorators and their metadata."""

    _instance = None
    _initialized = False

    def __new__(cls):
        """Implement singleton pattern."""
        if cls._instance is None:
            cls._instance = super(DecoratorRegistry, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize the registry."""
        if not DecoratorRegistry._initialized:
            self._decorators: Dict[str, Callable] = {}
            self._metadata: Dict[str, Dict[str, Any]] = {}
            self._dependencies: Dict[str, Set[str]] = {}
            self._decorated_functions: Dict[str, List[str]] = {}
            self._decorator_types: Dict[str, str] = {}  # Track decorator types (e.g., 'layer', 'validation')
            DecoratorRegistry._initialized = True

    def register(
        self,
        name: str,
        decorator: Callable,
        metadata: Optional[Dict[str, Any]] = None,
        dependencies: Optional[List[str]] = None,
        decorator_type: Optional[str] = None,
    ) -> None:
        """
        Register a new decorator with optional metadata and dependencies.

        Args:
            name: Unique name for the decorator
            decorator: The decorator function
            metadata: Optional metadata dictionary
            dependencies: Optional list of decorator names this decorator depends on
            decorator_type: Type of decorator (e.g., 'layer', 'validation')

        Raises:
            DecoratorError: If decorator with same name already exists or if invalid combination
        """
        if name in self._decorators:
            raise DecoratorError(f"Decorator {name} already registered")

        self._decorators[name] = decorator
        self._metadata[name] = metadata or {}
        self._dependencies[name] = set(dependencies or [])
        if decorator_type:
            self._decorator_types[name] = decorator_type

        # Validate decorator combinations
        if decorator_type == "layer":
            self._validate_layer_decorator(name, metadata)

    def _validate_layer_decorator(self, name: str, metadata: Dict[str, Any]) -> None:
        """
        Validate layer decorator registration.

        Args:
            name: Name of the decorator
            metadata: Metadata dictionary

        Raises:
            DecoratorError: If invalid layer decorator configuration
        """
        if "layer" not in metadata:
            raise DecoratorError(f"Layer decorator {name} must specify a layer")

        # Check for conflicting layer decorators
        for existing_name, existing_type in self._decorator_types.items():
            if existing_type == "layer" and self._metadata[existing_name]["layer"] == metadata["layer"]:
                # Allow layer-specific decorators (bronze, silver, gold) to wrap the base medallion decorator
                if not (name.startswith("medallion_") or existing_name.startswith("medallion_")):
                    raise DecoratorError(f"Multiple decorators for layer {metadata['layer']} not allowed")

    def merge_metadata(self, decorators: List[str]) -> Dict[str, Any]:
        """
        Merge metadata from multiple decorators.

        Args:
            decorators: List of decorator names to merge metadata from

        Returns:
            Merged metadata dictionary

        The merge follows these rules:
        1. Layer-specific metadata takes precedence
        2. Lists (e.g., expectations, metrics) are concatenated
        3. Dictionaries (e.g., table_properties) are merged with later values overriding earlier ones
        4. Scalar values are overridden by later decorators
        """
        merged: Dict[str, Any] = {}
        list_fields = {"expectations", "metrics"}
        dict_fields = {"table_properties", "options"}

        # Sort decorators to ensure layer decorators are processed last
        sorted_decorators = sorted(
            decorators,
            key=lambda d: 0 if self._decorator_types.get(d) == "layer" else 1
        )

        for decorator in sorted_decorators:
            metadata = self._metadata[decorator]
            for key, value in metadata.items():
                if key in list_fields and value:
                    if key not in merged:
                        merged[key] = []
                    merged[key].extend(value)
                elif key in dict_fields and value:
                    if key not in merged:
                        merged[key] = {}
                    merged[key].update(value)
                else:
                    merged[key] = value

        return merged

    def get_decorator(self, name: str) -> Callable:
        """
        Retrieve a registered decorator.

        Args:
            name: Name of the decorator to retrieve

        Returns:
            The decorator function

        Raises:
            DecoratorError: If decorator is not found
        """
        if name not in self._decorators:
            raise DecoratorError(f"Decorator {name} not found")
        return self._decorators[name]

    def get_metadata(self, name: str) -> Dict[str, Any]:
        """
        Get metadata for a decorator.

        Args:
            name: Name of the decorator

        Returns:
            Metadata dictionary for the decorator

        Raises:
            DecoratorError: If decorator is not found
        """
        if name not in self._metadata:
            raise DecoratorError(f"Decorator {name} not found")
        return self._metadata[name]

    def resolve_dependencies(self, decorators: List[str]) -> List[str]:
        """
        Resolve decorator execution order based on dependencies and types.

        Args:
            decorators: List of decorator names to resolve

        Returns:
            List of decorator names in correct execution order

        Raises:
            DecoratorError: If circular dependency is detected

        The order follows these rules:
        1. Validation decorators run first
        2. Transformation decorators run next
        3. Layer decorators run last
        4. Within each group, dependencies are respected
        """
        # Group decorators by type
        groups = {
            "validation": [],
            "transformation": [],
            "layer": [],
            "other": []
        }

        for decorator in decorators:
            decorator_type = self._decorator_types.get(decorator, "other")
            if decorator_type in groups:
                groups[decorator_type].append(decorator)
            else:
                groups["other"].append(decorator)

        # Process each group in order
        ordered: List[str] = []
        for group in ["validation", "transformation", "layer", "other"]:
            if groups[group]:
                # Resolve dependencies within the group
                visited: Set[str] = set()
                temp_visited: Set[str] = set()

                def visit(decorator: str) -> None:
                    if decorator in temp_visited:
                        raise DecoratorError(f"Circular dependency detected involving {decorator}")
                    if decorator in visited:
                        return

                    temp_visited.add(decorator)
                    for dep in self._dependencies.get(decorator, set()):
                        if dep not in self._decorators:
                            raise DecoratorError(f"Dependency {dep} not found")
                        # Only process dependencies in the same group
                        if dep in groups[group]:
                            visit(dep)
                    temp_visited.remove(decorator)
                    visited.add(decorator)
                    ordered.append(decorator)

                for decorator in groups[group]:
                    if decorator not in visited:
                        visit(decorator)

        return ordered

    def register_decorated_function(self, func_name: str, decorator_name: str) -> None:
        """
        Register that a function has been decorated.

        Args:
            func_name: Name of the decorated function
            decorator_name: Name of the decorator applied
        """
        if func_name not in self._decorated_functions:
            self._decorated_functions[func_name] = []
        self._decorated_functions[func_name].append(decorator_name)

    def get_function_decorators(self, func_name: str) -> List[str]:
        """
        Get all decorators applied to a function.

        Args:
            func_name: Name of the function

        Returns:
            List of decorator names applied to the function
        """
        return self._decorated_functions.get(func_name, []) 