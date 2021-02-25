"""PowerBIMetadata tap class."""

from pathlib import Path
from typing import List
import click
from singer_sdk import TapBase
from singer_sdk.helpers.typing import (
    ArrayType,
    BooleanType,
    ComplexType,
    DateTimeType,
    IntegerType,
    PropertiesList,
    StringType,
)

from tap_powerbi_metadata.stream import (
    TapPowerBIMetadataStream,
    ActivityEventsStream,
)

PLUGIN_NAME = "tap-powerbi-metadata"

STREAM_TYPES = [
    ActivityEventsStream,
]

class TapPowerBIMetadata(TapBase):
    """PowerBIMetadata tap class."""

    name = "tap-powerbi-metadata"
    config_jsonschema = PropertiesList(
        StringType("tenant_id"),
        StringType("user_name"),
        StringType("user_password"),
        DateTimeType("start_date"),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

# CLI Execution:

cli = TapPowerBIMetadata.cli
