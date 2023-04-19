"""Stream class for tap-powerbi-metadata."""

from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
from urllib import parse
import requests


from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIAuthenticatorBase, SimpleAuthenticator, OAuthAuthenticator, OAuthJWTAuthenticator
from singer_sdk.typing import (
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
            starting_datetime = self.get_starting_timestamp(partition)
            continuationToken = None
        if continuationToken:
            params["continuationToken"] = "'" + continuationToken + "'"
        else:
            params.update({"startDateTime": starting_datetime.strftime(API_DATE_FORMAT)})
            ending_datetime = starting_datetime.replace(hour=0, minute=0, second=0) + timedelta(days=1) + timedelta(microseconds=-1)
            params.update({"endDateTime": ending_datetime.strftime(API_DATE_FORMAT)})
        self.logger.debug(params)
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
            self.logger.debug("Params: {}".format(req_params))
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
                self.logger.debug(next_page_token)
            else:
                self.logger.info("No continuationToken, and nextUrlStartDate after today, calling it quits")
                return None
        return next_page_token

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
        # Keys
        Property("Id", StringType, required=True),
        Property("CreationTime", DateTimeType, required=True),
        # Properties
        Property("AccessRequestMessage", StringType),
        Property("AccessRequestType", StringType),
        Property("Activity", StringType),
        Property("ActivityId", StringType),
        Property(
            "AggregatedWorkspaceInformation",
            ObjectType(
                Property("WorkspaceCount", IntegerType),
                Property("WorkspacesByCapacitySku", StringType),
                Property("WorkspacesByType", StringType),
            )
        ),
        Property("AppName", StringType),
        Property("AppId", StringType),
        Property("AppReportId", StringType),
        Property("ArtifactId", StringType),
        Property("ArtifactKind", StringType),
        Property("ArtifactName", StringType),
        Property("ArtifactObjectId", StringType),
        Property("AuditedArtifactInformation",
            ObjectType(
                Property("AnnotatedItemType", StringType),
                Property("ArtifactObjectId", StringType),
                Property("Id", StringType),
                Property("Name", StringType),
            )
        ),
        Property("CapacityId", StringType),
        Property("CapacityName", StringType),
        Property("CapacityState", StringType),
        Property("CapacityUsers", StringType),
        Property("ClientIP", StringType),
        Property("ConsumptionMethod", StringType),
        Property("CopiedReportId", StringType),
        Property("CopiedReportName", StringType),
        Property("CredentialSetupMode", StringType),
        Property("CustomVisualAccessTokenResourceId", StringType),
        Property("CustomVisualAccessTokenSiteUri", StringType),
        Property("DashboardId", StringType),
        Property("DashboardName", StringType),
        Property("DataConnectivityMode", StringType),
        Property(
            "DataflowAccessTokenRequestParameters",
            ObjectType(
                Property("entityName", StringType),
                Property("partitionUri", StringType),
                Property("permissions", IntegerType),
                Property("tokenLifetimeInMinutes", IntegerType),
            )
        ),
        Property("DataflowAllowNativeQueries", BooleanType),
        Property("DataflowId", StringType),
        Property("DataflowName", StringType),
        Property("DataflowRefreshScheduleType", StringType),
        Property("DataflowType", StringType),
        Property("DatasetCertificationStage", StringType),
        Property("DatasetId", StringType),
        Property("DatasetName", StringType),
        Property(
            "Datasets",
            ArrayType(
                ObjectType(
                    Property("DatasetId", StringType),
                    Property("DatasetName", StringType),
                )
            )
        ),
        Property("DatasourceId", StringType),
        Property("DatasourceObjectIds", ArrayType(StringType)),
        Property("Datasources",  
            ArrayType(
                ObjectType(
                    Property("ConnectionDetails", StringType),
                    Property("DatasourceType", StringType),
                )
            )
        ),
        Property("DatasourceType", StringType),
        Property(
            "DeploymentPipelineAccesses",
            ArrayType(
                ObjectType(
                    Property("RolePermissions", StringType),
                    Property("UserObjectId", StringType),
                )
            )
        ),
        Property("DeploymentPipelineDisplayName", StringType),
        Property("DeploymentPipelineId", IntegerType),
        Property("DeploymentPipelineObjectId", StringType),
        Property("DeploymentPipelineStageOrder", IntegerType),
        Property("DistributionMethod", StringType),
        Property("EndPoint", StringType),
        Property("Experience", StringType),
        Property(
            "ExportedArtifactInfo",
            ObjectType(
                Property("ArtifactId", IntegerType),
                Property("ArtifactType", StringType),
                Property("ExportType", StringType),
            )
        ),
        Property("ExportEventActivityTypeParameter", StringType),
        Property("ExportEventEndDateTimeParameter", DateTimeType),
        Property("ExportEventStartDateTimeParameter", DateTimeType),
        Property("ExternalSubscribeeInformation", StringType),
        Property(
           "ExternalSubscribeeInformation",
            ArrayType(
                ObjectType(
                    Property("RecipientEmail", StringType),
                )
            )
        ),
        Property(
            "FolderAccessRequests",
            ArrayType(
                ObjectType(
                    Property("RolePermissions", StringType),
                    Property("UserObjectId", StringType),
                )
            )
        ),
        Property("FolderDisplayName", StringType),
        Property("FolderObjectId", StringType),
        Property("GatewayClusterId", StringType),
        Property(
            "GatewayClusters",
            ArrayType(
                ObjectType(
                    Property("id", StringType),
                    Property("memberGatewaysIds", ArrayType(StringType)),
                    Property(
                        "permissions",
                        ArrayType(
                            ObjectType(
                                Property("allowedDataSources", ArrayType(StringType)),
                                Property("id", StringType),
                                Property("principalType", StringType),
                                Property("role", StringType),
                            )
                        )
                    ),
                    Property("type", StringType),
                )
            )
        ),
        Property("GatewayId", StringType),
        Property("GatewayMemberId", StringType),
        Property("GatewayType", StringType),
        Property(
            "GenerateScreenshotInformation",
            ObjectType(
                Property("ExportFormat", StringType),
                Property("ExportType", IntegerType),
                Property("ExportUrl", StringType),
                Property("ScreenshotEngineType", IntegerType),
            )
        ),
        Property("HasFullReportAttachment", BooleanType),
        Property("ImportDisplayName", StringType),
        Property("ImportId", StringType),
        Property("ImportSource", StringType),
        Property("ImportType", StringType),
        Property("InstallTeamsAnalyticsInformation",
            ObjectType(
                Property("ModelId", StringType),
                Property("TenantId", StringType),
                Property("UserId", StringType),
            )
        ),
        Property("IsSuccess", BooleanType),
        Property("IsTemplateAppFromMarketplace", BooleanType),
        Property("IsTenantAdminApi", BooleanType),
        Property("IsUpdateAppActivity", BooleanType),
        Property("ItemName", StringType),
        Property("LastRefreshTime", StringType),
        Property("MembershipInformation", 
            ArrayType(
                ObjectType(
                    Property("MemberEmail", StringType),
                )
            )
        ),
        Property("MentionedUsersInformation", StringType),
        Property("ModelId", StringType),
        Property("ModelsSnapshots", ArrayType(IntegerType)),
        Property("Monikers", ArrayType(StringType)),
        Property("ObjectDisplayName", StringType),
        Property("ObjectId", StringType),
        Property("ObjectType", StringType),
        Property("Operation", StringType),
        Property("OrganizationId", StringType),
        Property(
            "OrgAppPermission",
            ObjectType(
                Property("permissions", StringType),
                Property("recipients", StringType),
            )
        ),
        Property("OriginalOwner", StringType),
        Property(
            "PaginatedReportDataSources",
            ArrayType(
                ObjectType(
                    Property("connectionString", StringType),
                    Property("credentialRetrievalType", StringType),
                    Property("", StringType),
                    Property("name", StringType),
                    Property("provider", StringType),
                )
            )
        ),
        Property("PinReportToTabInformation", 
            ObjectType(
                Property("ChannelId", StringType),
                Property("ChannelName", StringType),
                Property("DatasetId", StringType),
                Property("DatasetName", StringType),
                Property("ReportId", StringType),
                Property("ReportName", StringType),
                Property("TabName", StringType),
                Property("TeamId", StringType),
                Property("TeamName", StringType),
                Property("TeamsAppId", StringType),
                Property("UserId", StringType),
            )
        ),
        Property("RecordType", IntegerType),
        Property("RefreshType", StringType),
        Property("ReportCertificationStage", StringType),
        Property("ReportId", StringType),
        Property("ReportName", StringType),
        Property("ReportType", StringType),
        Property("RequestId", StringType),
        Property("ResultStatus", StringType),
        Property(
            "Schedules",
            ObjectType(
                Property("Days", ArrayType(StringType)),
                Property("RefreshFrequency", StringType),
                Property("Time", ArrayType(StringType)),
                Property("TimeZone", StringType),
            )
        ),
        Property("ShareLinkId", StringType),
        Property("SharingAction", StringType),
        Property(
            "SharingInformation",
            ArrayType(
                ObjectType(
                    Property("RecipientEmail", StringType),
                    Property("ResharePermission", StringType),
                )
            )
        ),
        Property("SharingScope", StringType),
        Property("SwitchState", StringType),
        Property(
            "SubscribeeInformation",
            ArrayType(
                ObjectType(
                    Property("ObjectId", StringType),
                    Property("RecipientEmail", StringType),
                    Property("RecipientName", StringType),
                )
            )
        ),
        Property(
            "SubscriptionSchedule",
            ObjectType(
                Property("DaysOfTheMonth",StringType),
                Property("EndDate", DateTimeType),
                Property("StartDate", DateTimeType),
                Property(
                    "Time",
                    ArrayType(StringType)
                ),
                Property("TimeZone", StringType),
                Property("Type", StringType),
                Property(
                    "WeekDays",
                    ArrayType(StringType)
                ),
            )
        ),
        Property("TableName", StringType),
        Property("TakingOverOwner", StringType),
        Property("TargetWorkspaceId", StringType),
        Property("TemplateAppFolderObjectId", StringType),
        Property("TemplateAppIsInstalledWithAutomation", BooleanType),
        Property("TemplateAppObjectId", StringType),
        Property("TemplateAppOwnerTenantObjectId", StringType),
        Property("TemplateAppVersion", StringType),
        Property("TemplatePackageName", StringType),
        Property("TileText", StringType),
        Property(
            "UpdateFeaturedTables",
            ArrayType(
                ObjectType(
                    Property("State", StringType),
                    Property("TableName", StringType),
                )
            )
        ),
        Property("UserAgent", StringType),
        Property("UserId", StringType),
        Property(
            "UserInformation",
            ObjectType(
                Property(
                    "UsersAdded",
                    ArrayType(StringType)
                ),
                Property(
                    "UsersRemoved",
                    ArrayType(StringType)
                ),
            )
        ),
        Property("UserKey", StringType),
        Property("UserType", IntegerType),
        Property("Workload", StringType),
        Property(
            "WorkspaceAccessList", 
            ArrayType(
                ObjectType(
                    Property(
                        "UserAccessList",
                        ArrayType(
                            ObjectType(
                                Property("GroupUserAccessRight", StringType),
                                Property("Identifier", StringType),
                                Property("PrincipalType", StringType),
                                Property("UserEmailAddress", StringType),
                            )
                        ),
                    ),
                    Property("WorkspaceId", StringType),
                )
            )
        ),
        Property("WorkspaceId", StringType),
        Property("WorkSpaceName", StringType),
        Property("WorkspacesSemicolonDelimitedList", StringType),
    ).to_dict()
