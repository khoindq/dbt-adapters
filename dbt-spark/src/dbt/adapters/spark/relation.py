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

        When catalog is specified and not 'default', render as: catalog.schema.table
        Otherwise, render as: schema.table (backward compatible)
        """
        # Build the relation name with catalog
        parts = []

        # Add catalog if present and not default
        if self.include_catalog():
            parts.append(self.quoted(self.catalog))

        # Add schema
        if self.include_policy.schema and self.schema:
            parts.append(self.quoted(self.schema))

        # Add identifier
        if self.include_policy.identifier and self.identifier:
            parts.append(self.quoted(self.identifier))

        return ".".join(parts)

    def include_catalog(self) -> bool:
        """
        Determine if catalog should be included in the rendered relation name.

        Returns True if:
        - catalog is set
        - catalog is not 'default' (case-insensitive)
        - catalog is not empty
        """
        if not self.catalog:
            return False
        if self.catalog.lower() == "default":
            return False
        return True
