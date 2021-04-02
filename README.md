# tap-basecone

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Basecone](https://developers.basecone.com/ApiReference/General)
- Extracts the following resources:
  - Transaction Collection
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

### Step 1: Register your app

This pipeline needs to have access to the reports. This is done by going to [their website](https://developers.basecone.com/) and clicking `Register`

### Step 2: Configure

Create a file called `basecone_config.json` in your working directory, following [sample_basecone_config.json](sample_basecone_config.json). The required parameters are the `company_id` and `auth_token`. More information about authentication can be found in the [API Reference](https://developers.basecone.com/ApiReference/Authentication)

This requires a `state.json` file to let the tap know from when to retrieve data. For example:
```
{
  "bookmarks": {
    "transaction_collection": {
      "start_date": "2015-01-01"
    }
  }
}
```
Will replicate transaction data from 2015-01-01.

### Step 3: Install and Run

Create a virtual Python environment for this tap. This tap has been tested with Python 3.7, 3.8 and 3.9 and might run on future versions without problems.
```
python -m venv singer-basecone
singer-twinfield/bin/python -m pip install --upgrade pip
singer-twinfield/bin/pip install git+https://github.com/Yoast/singer-tap-basecone.git
```

This tap can be tested by piping the data to a local JSON target. For example:

Create a virtual Python environment with `singer-json`
```
python -m venv singer-json
singer-json/bin/python -m pip install --upgrade pip
singer-json/bin/pip install target-json
```

Test the tap:

```
singer-basecone/bin/tap-basecone --state state.json -c basecone_config.json | singer-json/bin/target-json >> state_result.json
```

Copyright &copy; 2021 Yoast
