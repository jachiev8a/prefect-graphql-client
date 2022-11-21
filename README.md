# prefect-graphql-client

## Introduction
This prefect client has some utils to interact with the Prefect Cloud service.
These utils include reporting to check all scheduled workflows in place,
showing if they are active or not and what cron configuration they have.

## Installation

### Environment
```bash
# copy .env.example to .env
cp .env.example .env

# This example file has 2 variables as placeholders.
# You need to edit these variables with actual values from cloud.
PREFECT_API_KEY=xXxXx
PREFECT_TENANT_ID=xXxXx

# NOTE: get the api key and the tenant ID from the cloud environment
```

### Python
```bash
# create python virtual env
python -m virtualenv .venv

# activate virtualenv (linux)
. .venv/bin/activate

# (venv) install dependencies
python -m pip install -r requirements.txt
```

## How to use the client

```bash
# activate virtualenv (linux)
. .venv/bin/activate

# (venv) run main script with help command 
# to see what you are able to do with it.
python query_executor.py -h

# Examples:
python query_executor.py --print-schedule-active
python query_executor.py --print-schedule-config
python query_executor.py --print-general-report
```

## Usages/Examples

```bash
# activate virtualenv (linux)
. .venv/bin/activate

# (venv) run main script with help command 
# to see what you are able to do with it.
# short arguments flags are used:
#   -r for "--print-general-report"
#   -p for "--project-filter"
python query_executor.py -r -p "prod"

# this will print a general report, filtering projects
# that contains *prod* name in the project name. 
```
