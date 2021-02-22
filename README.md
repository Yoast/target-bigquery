# target-bigquery

A [Singer](https://singer.io) target that writes data to Google BigQuery. This fork has been fully rewritten by Yoast and contains fixes for streaming data. Schema: Numbers are default Numeric in BigQuery

## How to use it

`target-bigquery` works together with any other [Singer Tap] to move data from sources like [Braintree], [Freshdesk] and [Hubspot] to Google BigQuery. 

### Step 1: Activate the Google BigQuery API

 (originally found in the [Google API docs](https://googlecloudplatform.github.io/google-cloud-python/latest/bigquery/usage.html))
 
 1. Use [this wizard](https://console.developers.google.com/start/api?id=bigquery-json.googleapis.com) to create or select a project in the Google Developers Console and activate the BigQuery API. Click Continue, then Go to credentials.
 1. On the **Add credentials to your project** page, click the **Cancel** button.
 1. At the top of the page, select the **OAuth consent screen** tab. Select an **Email address**, enter a **Product name** if not already set, and click the **Save** button.
 1. Select the **Credentials** tab, click the **Create credentials** button and select **OAuth client ID**.
 1. Select the application type **Other**, enter the name "Singer BigQuery Target", and click the **Create** button.
 1. Click **OK** to dismiss the resulting dialog.
 1. Click the Download button to the right of the client ID.
 1. Move this file to your working directory and rename it *client_secrets.json*.

### Step 2: Configure

Create a file called `bigquery_config.json` in your working directory, following [config.json.example](config.json.example). The required parameters are the project name `project_id` and the dataset name `dataset_id`. 

### Step 3: Install and Run

First, make sure Python 3 is installed on your system or follow these installation instructions for [Mac](python-mac) or [Ubuntu](python-ubuntu). This target has been tested with Python 3.7, 3.8 and 3.9 and might run on future versions without problems.

`target-bigquery` can be run with any [Singer Tap], but we'll use [`tap-fixerio`][Fixerio] - which pulls currency exchange rate data from a public data set - as an example.

These commands will install `tap-fixerio` and `target-bigquery` with pip and then run them together, piping the output of `tap-fixerio` to `target-bigquery`:

```bash
› pip install target-bigquery tap-fixerio
› tap-fixerio | target-bigquery -c config.json
  INFO Replicating the latest exchange rate data from fixer.io
  INFO Tap exiting normally
```

If you're using a different Tap, substitute `tap-fixerio` in the final command above to the command used to run your Tap.

### Authentication

It is recommended to use `target-bigquery` with a service account.
* Download the client_secrets.json file for your service account, and place it on the machine where `target-bigquery` will be executed.
* Set a `GOOGLE_APPLICATION_CREDENTIALS` environment variable on the machine, where the value is the fully qualified path to client_secrets.json

It should be possible to use the oAuth flow to authenticate to GCP as well:
* `target-bigquery` will attempt to open a new window or tab in your default browser. If this fails, copy the URL from the console and manually open it in your browser.
* If you are not already logged into your Google account, you will be prompted to log in.
* If you are logged into multiple Google accounts, you will be asked to select one account to use for the authorization.
* Click the **Accept** button to allow `target-bigquery` to access your Google BigQuery table.
* You can close the tab after the signup flow is complete.

The data will be written to the table specified in your `config.json`.

### Schema
By default, this target will convert numbers to NUMERIC types in BigQuery. If you want to use a different type, use the format below:

To convert numbers to float:
```
"propety": {
  "type": "number",
  "format": "float" # float, integer, numeric, bignumeric
}
```

Copyright &copy; 2021 Yoast