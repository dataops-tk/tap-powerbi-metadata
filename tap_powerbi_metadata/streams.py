"""Stream class for tap-powerbi-metadata."""

from copy import deepcopy
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
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
    NumberType,
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
            'username': self.config["username"],
            'password': self.config["password"],
        }


class TapPowerBIMetadataStream(RESTStream):
    """PowerBIMetadata stream class."""

    url_base = "https://api.powerbi.com/v1.0/myorg"

    def get_url_params(self, partition: Optional[dict]) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = {}
        starting_datetime = self.get_starting_datetime(partition)
        if starting_datetime:
            params.update({"startDateTime": starting_datetime.strftime("'%Y-%m-%dT%H:%M:%SZ'")})
            #TODO need to loop over multiple days until done (https://github.com/dataops-tk/tap-powerbi-metadata/issues/5)
            ending_datetime = starting_datetime + timedelta(hours=9)
            params.update({"endDateTime": ending_datetime.strftime("'%Y-%m-%dT%H:%M:%SZ'")})
        self.logger.info(params)
        return params

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        return OAuthActiveDirectoryAuthenticator(
            stream=self,
            auth_endpoint=f"https://login.microsoftonline.com/{self.config['tenant_id']}/oauth2/token",
            oauth_scopes="https://analysis.windows.net/powerbi/api",
        )

    def get_next_page_token(self, response: requests.Response) -> Optional[Any]:
        """Return token for identifying next page or None if not applicable."""
        #TODO Continuation Token not properly parsed (https://github.com/dataops-tk/tap-powerbi-metadata/issues/4)
        #resp_json = response.json()
        #self.logger.info(resp_json.get("continuationUri"))
        #return resp_json.get("continuationToken")
        return None

    def insert_next_page_token(self, next_page: Any, params: dict) -> Any:
        """Inject next page token into http request params."""
        if (not next_page) or next_page == 1:
            return params
        params.pop("startDateTime")
        params.pop("endDateTime")
        params["continuationToken"] = "'" + next_page + "'"
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        resp_json = response.json()
        for row in resp_json.get("activityEventEntities"):
            yield row


class ActivityEventsStream(TapPowerBIMetadataStream):
    name = "ActivityEvents"
    path = "/admin/activityevents"
    primary_keys = ["Id"]
    replication_key = "CreationTime"
    schema = PropertiesList(
        StringType("Id", required=True),
        IntegerType("RecordType"),
        DateTimeType("CreationTime", required=True),
        StringType("Operation"),
        StringType("OrganizationId"),
        IntegerType("UserType"),
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
        StringType("DataflowId"),
        StringType("DataflowName"),
        StringType("DataflowType"),
        StringType("DataflowAccessTokenRequestParameters"),
        StringType("CustomVisualAccessTokenResourceId"),
        StringType("CustomVisualAccessTokenSiteUri"),
        StringType("ExportedArtifactInfo"),
        StringType("DataConnectivityMode"),
        StringType("LastRefreshTime"),
        StringType("Schedules"),
        StringType("ImportId"),
        StringType("ImportType"),
        StringType("ImportSource"),
        StringType("ImportDisplayName"),
        StringType("RefreshType"),
        StringType("DashboardId"),
        StringType("DashboardName"),
        StringType("Datasets"),
        StringType("ModelsSnapshots"),
        StringType("OrgAppPermission"),
    ).to_dict()
