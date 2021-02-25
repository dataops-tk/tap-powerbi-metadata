"""Stream class for tap-powerbi-metadata."""

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict

from tap_base.streams import RESTStreamBase
from tap_base.authenticators import APIAuthenticatorBase, SimpleAuthenticator, OAuthAuthenticator, OAuthJWTAuthenticator
from singer_sdk.helpers.typing import (
    ArrayType,
    BooleanType,
    ComplexType,
    DateTimeType,
    IntegerType,
    PropertiesList,
    StringType,
)


class TapPowerBIMetadataStream(RESTStreamBase):
    """PowerBIMetadata stream class."""

    url_base = "https://api.powerbi.com/v1.0/myorg"

    def get_url_params(self, partition: Optional[dict]) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        state = self.get_stream_or_partition_state(partition)
        result = deepcopy(state)
        result.update({"start_date": self.config.get("start_date")})
        return result

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        return OAuthAuthenticator(
            stream=self,
            auth_endpoint="TODO: OAuth Endpoint URL",
            oauth_scopes="TODO: OAuth Scopes",
        )


class ActivityEventsStream(TapPowerBIMetadataStream):
    stream_name = "ActivityEvents"
    path = "/admin/activityevents"
    primary_keys = ["Id"]
    replication_key = "CreationTime"
    schema = PropertiesList(
        StringType("Id"),
        StringType("RecordType"),
        DateTimeType("CreationTime"),
        StringType("Operation"),
        StringType("OrganizationId"),
        StringType("UserType"),
        StringType("UserKey"),
        StringType("Workload"),
        StringType("UserId"),
        StringType("ClientIP"),
        StringType("UserAgent"),
        StringType("Activity"),
        StringType("ItemName"),
        StringType("CapacityId"),
        StringType("CapacityName"),
        StringType("WorkspaceId"),
        StringType("WorkSpaceName"),
        StringType("DatasetId"),
        StringType("DatasetName"),
        StringType("ReportId"),
        StringType("ReportName"),
        StringType("ObjectId"),
        BooleanType("IsSuccess"),
        StringType("ReportType"),
        StringType("RequestId"),
        StringType("ActivityId"),
        StringType("AppName"),
        StringType("AppReportId"),
        StringType("DistributionMethod"),
        StringType("ConsumptionMethod"),
    ).to_dict()
