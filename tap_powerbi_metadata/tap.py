"""PowerBIMetadata tap class."""

from pathlib import Path
from typing import List
import click
from singer_sdk import Tap, Stream
from singer_sdk.helpers.typing import (
    ArrayType,
    BooleanType,
    ComplexType,
    DateTimeType,
    IntegerType,
    NumberType,
    PropertiesList,
    StringType,
)

from tap_powerbi_metadata.streams import (
    TapPowerBIMetadataStream,
    ActivityEventsStream,
)

PLUGIN_NAME = "tap-powerbi-metadata"

STREAM_TYPES = [
    ActivityEventsStream,
]

class TapPowerBIMetadata(Tap):
    """PowerBIMetadata tap class."""

    name = "tap-powerbi-metadata"
    config_jsonschema = PropertiesList(
        StringType("tenant_id", required=True),
        StringType("client_id", required=True),
        StringType("username", required=True),
        StringType("password", required=True),
        DateTimeType("start_date"),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

# CLI Execution:

cli = TapPowerBIMetadata.cli
