"""Stream class for tap-powerbi-metadata."""

from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
from urllib import parse
import requests
from singer_sdk.helpers.util import utc_now


from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIAuthenticatorBase, SimpleAuthenticator, OAuthAuthenticator, OAuthJWTAuthenticator
from singer_sdk.helpers.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

API_DATE_FORMAT = "'%Y-%m-%dT%H:%M:%SZ'"

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

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.
        
        API only supports a single UTC day, or continuationToken-based pagination.
        """
        params = {}
        if next_page_token:
            starting_datetime = next_page_token["urlStartDate"]
            continuationToken = next_page_token.get("continuationToken")
        else:
            starting_datetime = self.get_starting_datetime(partition)
            continuationToken = None
        if continuationToken:
            params["continuationToken"] = "'" + continuationToken + "'"
        else:
            params.update({"startDateTime": starting_datetime.strftime(API_DATE_FORMAT)})
            ending_datetime = starting_datetime.replace(hour=0, minute=0, second=0) + timedelta(days=1) + timedelta(microseconds=-1)
            params.update({"endDateTime": ending_datetime.strftime(API_DATE_FORMAT)})
        self.logger.info(params)
        return params

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        return OAuthActiveDirectoryAuthenticator(
            stream=self,
            auth_endpoint=f"https://login.microsoftonline.com/{self.config['tenant_id']}/oauth2/token",
            oauth_scopes="https://analysis.windows.net/powerbi/api",
        )

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[Any] = None) -> Optional[Any]:
        """Return token for identifying next page or None if not applicable."""
        resp_json = response.json()
        continuationToken = resp_json.get("continuationToken")
        next_page_token = {}
        if not previous_token:
            # First time creating a pagination token so we need to record the initial start date.
            req_url = response.request.url
            req_params = parse.parse_qs(parse.urlparse(req_url).query)
            self.logger.info("Params: {}".format(req_params))
            latest_url_start_date_param = req_params["startDateTime"][0]
            next_page_token["urlStartDate"] = datetime.strptime(latest_url_start_date_param, API_DATE_FORMAT)
        else: 
            next_page_token["urlStartDate"] = previous_token.get("urlStartDate")
        if continuationToken:
            next_page_token["continuationToken"] = requests.utils.unquote(continuationToken)
        else:
            next_page_token["continuationToken"] = None
            # Now check if we should repeat API call for next day
            latestUrlStartDate = next_page_token["urlStartDate"]
            nextUrlStartDate = latestUrlStartDate.replace(hour=0, minute=0, second=0) + timedelta(days=1)
            self.logger.info("No next page token found, checking if {} is greater than now".format(nextUrlStartDate))
            if nextUrlStartDate < datetime.utcnow():
                self.logger.info("{} is less than now, incrementing date by 1 and continuing".format(nextUrlStartDate))
                next_page_token["urlStartDate"] = nextUrlStartDate
                self.logger.info(next_page_token)
            else:
                self.logger.info("No continuationToken, and nextUrlStartDate after today, calling it quits")
                return None
        return next_page_token

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        resp_json = response.json()
        for row in resp_json.get("activityEventEntities"):
            yield row

    # Hack until core.py in SDK is updated
    
    def get_starting_datetime(self, partition: Optional[dict]) -> Optional[datetime]:
        import pendulum
        result: Optional[datetime] = None
        if self.is_timestamp_replication_key or True:
            state = self.get_stream_or_partition_state(partition)
            if "replication_key_value" in state:
                result = pendulum.parse(state["replication_key_value"])
        if result is None and "start_date" in self.config:
            result = pendulum.parse(self.config.get("start_date"))
        return result

    
    
    
    
    # def request_records(self, partition: Optional[dict]) -> Iterable[dict]:
    #     """Request records from REST endpoint(s), returning an iterable Dict of response records.

    #     If pagination can be detected, pages will be recursed automatically.
    #     """
    #     next_page_token = 1
    #     while next_page_token:
    #         self.logger.info(partition)
    #         self.logger.info(self.get_starting_datetime(partition))
    #         prepared_request = self.prepare_request(partition, next_page_token=next_page_token)
    #         resp = self._request_with_backoff(prepared_request)
    #         for row in self.parse_response(resp):
    #             yield row
    #         next_page_token = self.get_next_page_token(resp)


class ActivityEventsStream(TapPowerBIMetadataStream):
    name = "ActivityEvents"
    path = "/admin/activityevents"
    primary_keys = ["Id"]
    replication_key = "CreationTime"
    schema = PropertiesList(
        Property("Id", StringType, required=True),
        Property("RecordType", IntegerType),
        Property("CreationTime", DateTimeType, required=True),
        Property("Operation", StringType),
        Property("OrganizationId", StringType),
        Property("UserType", IntegerType),
        Property("UserKey", StringType),
        Property("Workload", StringType),
        Property("UserId", StringType),
        Property("ClientIP", StringType),
        Property("UserAgent", StringType),
        Property("Activity", StringType),
        Property("ItemName", StringType),
        Property("CapacityId", StringType),
        Property("CapacityName", StringType),
        Property("WorkspaceId", StringType),
        Property("WorkSpaceName", StringType),
        Property("DatasetId", StringType),
        Property("DatasetName", StringType),
        #GatewayId
        #DatasourceId
        Property("ReportId", StringType),
        Property("ReportName", StringType),
        Property("ObjectId", StringType),
        Property("IsSuccess", BooleanType),
        Property("ReportType", StringType),
        Property("RequestId", StringType),
        Property("ActivityId", StringType),
        Property("AppName", StringType),
        Property("AppReportId", StringType),
        Property("DistributionMethod", StringType),
        Property("ConsumptionMethod", StringType),
        Property("DataflowId", StringType),
        Property("DataflowName", StringType),
        Property("DataflowType", StringType),
        Property(
            "DataflowAccessTokenRequestParameters",
            ObjectType(
                Property("tokenLifetimeInMinutes", IntegerType),
                Property("permissions", IntegerType),
                Property("entityName", StringType),
                Property("partitionUri", StringType)
            )
        )        ,
        Property("CustomVisualAccessTokenResourceId", StringType),
        Property("CustomVisualAccessTokenSiteUri", StringType),
        Property(
            "ExportedArtifactInfo",
            ObjectType(
                Property("ExportType", StringType),
                Property("ArtifactType", StringType),
                Property("ArtifactId", IntegerType)
            )
        ),
        Property("DataConnectivityMode", StringType),
        Property("LastRefreshTime", StringType),
        Property(
            "Schedules",
            ObjectType(
                Property("RefreshFrequency", StringType),
                Property("TimeZone", StringType),
                Property("Days", ArrayType(StringType)),
                Property("Time", ArrayType(StringType))
            )
        ),
        Property("ImportId", StringType),
        Property("ImportType", StringType),
        Property("ImportSource", StringType),
        Property("ImportDisplayName", StringType),
        Property("RefreshType", StringType),
        Property("DashboardId", StringType),
        Property("DashboardName", StringType),
        Property(
            "Datasets",
            ArrayType(
                ObjectType(
                    Property("DatasetId", StringType),
                    Property("DatasetName", StringType)
                )
            )
        ),
        Property("ModelsSnapshots", ArrayType(IntegerType)),
        Property(
            "OrgAppPermission",
            ObjectType(
                Property("recipients", StringType),
                Property("permissions", StringType)
            )
        ),
        Property(
            "GenerateScreenshotInformation",
            ObjectType(
                Property("ExportType", IntegerType),
                Property("ScreenshotEngineType", IntegerType),
                Property("ExportFormat", StringType),
                Property("ExportUrl", StringType)
            )
        ),
        Property("SharingAction", StringType),
        Property(
            "SharingInformation",
            ArrayType(
                ObjectType(
                    Property("RecipientEmail", StringType),
                    Property("ResharePermission", StringType)
                )
            )
        ),
        Property("ArtifactId", StringType),
        Property("ArtifactName", StringType),
        Property("FolderObjectId", StringType),
        Property("FolderDisplayName", StringType),
        #FolderAccessRequests
        Property("ExportEventStartDateTimeParameter", StringType),
        Property("ExportEventEndDateTimeParameter", StringType),
        Property("ExportEventActivityTypeParameter", StringType)
        # CapacityUsers
        # CapacityState
        # DeploymentPipelineId
        # DeploymentPipelineObjectId
        # DeploymentPipelineDisplayName
        # DeploymentPipelineStageOrder
        # DeploymentPipelineAccesses
        # TileText
        # TableName
        # TemplateAppObjectId
        # TemplatePackageName
        # TemplateAppVersion
        # TemplateAppOwnerTenantObjectId
        # TemplateAppFolderObjectId
        # TemplateAppIsInstalledWithAutomation
        # IsTemplateAppFromMarketplace
        # IsUpdateAppActivity
    ).to_dict()
