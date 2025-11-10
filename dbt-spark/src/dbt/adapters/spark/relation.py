from typing import Optional, TypeVar
from dataclasses import dataclass, field

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.adapters.events.logging import AdapterLogger

from dbt_common.exceptions import DbtRuntimeError

logger = AdapterLogger("Spark")

Self = TypeVar("Self", bound="BaseRelation")


@dataclass
class SparkQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False
    catalog: bool = False


@dataclass
class SparkIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class SparkRelation(BaseRelation):
    quote_policy: Policy = field(default_factory=lambda: SparkQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: SparkIncludePolicy())
    quote_character: str = "`"
    is_delta: Optional[bool] = None
    is_hudi: Optional[bool] = None
    is_iceberg: Optional[bool] = None
    # TODO: make this a dict everywhere
    information: Optional[str] = None
    require_alias: bool = False

    def __post_init__(self) -> None:
        if self.database != self.schema and self.database:
            raise DbtRuntimeError("Cannot set database in spark!")

    def _quote_if_needed(self, value: str, should_quote: bool) -> str:
        """Quote a value if the policy requires it."""
        return self.quoted(value) if should_quote else str(value)

    def render(self) -> str:
        if self.include_policy.database and self.include_policy.schema:
            raise DbtRuntimeError(
                "Got a spark relation with schema and database set to "
                "include, but only one can be set"
            )

        # Use catalog-aware rendering if catalog is present
        if self.include_catalog():
            return self.render_limited()

        return super().render()

    def render_limited(self) -> str:
        """
        Render the relation name with catalog support.

        When catalog is specified, render as: catalog.schema.table (3-level namespace)
        Otherwise, render as: schema.table (2-level namespace, backward compatible)

        Respects the quote_policy for each component.
        """
        parts = []

        # Add catalog if present (3-level namespace)
        if self.include_catalog():
            parts.append(self._quote_if_needed(self.catalog, self.quote_policy.catalog))

        # Add schema
        if self.include_policy.schema and self.schema:
            parts.append(self._quote_if_needed(self.schema, self.quote_policy.schema))

        # Add identifier
        if self.include_policy.identifier and self.identifier:
            parts.append(self._quote_if_needed(self.identifier, self.quote_policy.identifier))

        return ".".join(parts)

    def include_catalog(self) -> bool:
        """
        Determine if catalog should be included in the rendered relation name.

        Returns True if catalog is set and not empty/whitespace.
        Returns False if catalog is None, empty string, or only whitespace.
        """
        return bool(self.catalog and self.catalog.strip())
