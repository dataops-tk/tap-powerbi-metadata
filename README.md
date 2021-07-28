# Welcome to the tap-powerbi-metadata Singer Tap!

This [Singer](https://singer.io) tap was created using the [Meletano SDK for Taps](https://sdk.meltano.com).

---------------------------------

## Installation

```bash
pipx install git+https://github.com/dataops-tk/tap-powerbi-metadata.git
```

## Configuration

### Accepted Config Options

- `client_id` - The unique client ID for the Power BI tenant.
- `tenant_id` - The unique identifier for the Power BI tenant.
- `username` - Username to use in the flow.
- `password` - Password to use in the auth flow.
- `start_date` - Optional. Earliest date of data to stream.

Note:

- A sample config file is available at `.secrets/config.json.template`
- A full list of supported settings and capabilities for this tap is available by running:

    ```bash
    tap-powerbi-metadata --about
    ```

### Source Authentication and Authorization

_**NOTE:** Access to the Power BI REST API requires a service account (aka "Service Principal"), which must
be created by someone with admin access to Azure Active Directory (AAD)._

More information on this process is available under the 
[Automation with service principals](https://docs.microsoft.com/en-us/power-bi/admin/service-premium-service-principal)
topic on docs.microsoft.com.

- If you do not have access to a Power BI tenant, this _might_ be helpful:
  - https://docs.microsoft.com/en-us/power-bi/developer/embedded/create-an-azure-active-directory-tenant

`TODO:` Test out this process of creating a new tenant and service principal for testing purposes and so users/developers won't have to run this in prod to know it works properly.


## Usage

You can easily run `tap-powerbi-metadata` by itself or in a pipeline using [Meltano](www.meltano.com).

### Executing the Tap Directly

```bash
tap-powerbi-metadata --version
tap-powerbi-metadata --help
tap-powerbi-metadata --config CONFIG --discover > ./catalog.json
```


## How to Contribute

See the [SDK dev guide](https://sdk.meltano.com/dev_guide.html) for more instructions on how to use the Singer SDK to develop your own taps and targets.

### Upgrading the SDK Version

To upgrade the version of SDK being used, go to the [Release History tab on the pypi repo for the SDK](https://pypi.org/project/singer-sdk/#history) and copy the version number only

Then in the command prompt, while in the repo run the following, after replacing the version number with the one you copied

```bash
poetry add singer-sdk==0.2.0
```

### Initialize your Development Environment

If you've not already installed Poetry:

```bash
pipx install poetry
```

To update your local virtual environment:

```bash
poetry install
```

### Testing locally

Execute the tap locally with the `poetry run` prefix:

```bash
poetry run tap-powerbi-metadata --help

poetry run tap-powerbi-metadata --config=.secrets\config.json > Activity.jsonl
cat Activity.jsonl | target-csv
cat Activity.jsonl | target-snowflake --config=.secrets/target-config.json
```

### Create and Run Tests

Create tests within the `tap-powerbi-metadata/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-powerbi-metadata` CLI interface directly using `poetry run`:

```bash
poetry run tap-powerbi-metadata --help
```
