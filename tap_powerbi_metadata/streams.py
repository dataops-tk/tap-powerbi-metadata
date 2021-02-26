"""Stream class for tap-powerbi-metadata."""

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional
import requests
from singer_sdk.helpers.util import utc_now


from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIAuthenticatorBase, SimpleAuthenticator, OAuthAuthenticator, OAuthJWTAuthenticator
from singer_sdk.helpers.typing import (
    ArrayType,
    BooleanType,
    ComplexType,
    DateTimeType,
    IntegerType,
    PropertiesList,
    StringType,
)


class OAuthActiveDirectoryAuthenticator(OAuthAuthenticator):
    # https://pivotalbi.com/automate-your-power-bi-dataset-refresh-with-python

    @property
    def oauth_request_body(self) -> dict:
        return {
            'grant_type': 'password',
            'scope': 'https://api.powerbi.com',
            'resource': 'https://analysis.windows.net/powerbi/api',
            'client_id': self.config["client_id"],
            'username': self.config.get("username", self.config["client_id"]),
            'password': self.config["password"],
        }


class TapPowerBIMetadataStream(RESTStream):
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
        return OAuthActiveDirectoryAuthenticator(
            stream=self,
            auth_endpoint=f"https://login.microsoftonline.com/{self.config['tenant_id']}/oauth2/token",
            oauth_scopes="https://analysis.windows.net/powerbi/api",
        )

    def get_next_page_token(self, response: dict) -> Optional[Any]:
        """Return token for identifying next page or None if not applicable."""
        return response.get("continuationToken")

    def insert_next_page_token(self, next_page, params) -> Any:
        """Inject next page token into http request params."""
        if (not next_page) or next_page == 1:
            return params
        params["continuationToken"] = next_page
        return params

    def get_url_params(self, partition: Optional[dict] = None) -> dict:
        if "start_date" in self.config:
            return { "startDateTime": self.config.get("start_date") }
        return {}


class ActivityEventsStream(TapPowerBIMetadataStream):
    name = "ActivityEvents"
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
