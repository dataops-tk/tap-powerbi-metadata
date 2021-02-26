_**NOTE:** The Singer SDK framework is still in early exploration and development phases. For more
information and to be updated on this project, please feel free to subscribe to our
[original Meltano thread](https://gitlab.com/meltano/meltano/-/issues/2401) and the
[initial PR for the underlying framework](https://gitlab.com/meltano/singer-sdk/-/merge_requests/1)._

--------------------------------

# Welcome to the tap-powerbi-metadata Singer Tap!

This Singer-compliant tap was created using the [Singer SDK](https://gitlab.com/meltano/singer-sdk).

## Getting Started

`TODO: Delete this section after tap is tested and working`

- [ ] As a first step, you will want to scan the entire project for the text "`TODO:`" and complete any recommended steps.
- [ ] Once you have a boilerplate prepped, you'll want to setup Poetry and create the virtual environment for your project:

    ```bash
    pipx install poetry
    poetry install
    # Now navigate to your repo root and then run:
    poetry init
    ```

- [ ] You can test out your new CLI directly with:

    ```bash
    poetry run tap-powerbi-metadata --help
    ```

- [ ] Create some tests and then run:

    ```bash
    poetry run pytest
    ```

_`TODO: Remove the above section once complete.`_

## Singer SDK Dev Guide

See the [dev guide](../../docs/dev_guide.md) for more instructions on how to use the Singer SDK to 
develop your own taps and targets.

## Config Guide

### Accepted Config Options

- `client_id` - The unique client ID for the Power BI tenant.
- `tenant_id` - The unique identifier for the Power BI tenant.
- `username` - Username to use in the flow.
- `password` - Password to use in the auth flow.
- `start_date` - Optional. Earliest date of data to stream.

Note:

- A sample config file is available at `.secrets/config.json.template`

### Source Authentication and Authorization

_**NOTE:** Access to the Power BI REST API requires a service account (aka "Service Principal"), which must
be created by someone with admin access to Azure Active Directory (AAD)._

More information on this process is available under the 
[Automation with service principals](https://docs.microsoft.com/en-us/power-bi/admin/service-premium-service-principal)
topic on docs.microsoft.com.

- If you do not have access to a Power BI tenant, this _might_ be helpful:
  - https://docs.microsoft.com/en-us/power-bi/developer/embedded/create-an-azure-active-directory-tenant

`TODO:` Test out this process of creating a new tenant and service principal for testing purposes and so users/developers won't have to run this in prod to know it works properly.
