# BigData Jobs Profiling AI Agent

This repository contains a Python utility designed to profile and tune
Apache Spark jobs running on one or more Hadoop/YARN clusters.  It
automates the collection of job metadata, analyses resource
utilisation, generates resource tuning recommendations using
OpenAI’s ChatCompletion API and notifies stakeholders via Slack.  The
recommendations are stored in a MySQL database and committed to a Git
repository for version control.  The latest version supports
**multi‑cluster** deployments: you can define multiple Hadoop
clusters (e.g. `LAKE`, `STREAMING`, etc.) with their own YARN and
Spark History Server endpoints and the agent will profile each in
turn.

## Features

* **Metadata collection**: Fetches application details from the YARN
  ResourceManager REST API (`/ws/v1/cluster/apps`), including memory
  seconds, vcore seconds and elapsed time【762643672411904†L4727-L4851】.
  In multi‑cluster mode the agent iterates over all configured
  clusters and prefixes job IDs with the cluster name so you can
  distinguish jobs originating from different environments.
* **Executor metrics**: Retrieves per‑executor statistics from the
  Spark History Server REST API (`/api/v1/applications/[app‑id]/executors`) to
  augment job profiles【383517666583736†L417-L423】.  Metrics are
  stored in a per‑cluster context.
* **MySQL persistence**: Stores job metadata, Spark metrics and
  recommendations in a relational database.  Schema creation is
  automatic on first run.
* **Analysis and categorisation**: Computes average CPU and memory
  consumption and assigns jobs to SLA categories based on elapsed
  runtime.  Jobs are grouped into `hourly` (≤ 1 hour), `two_hours`
  (> 1 hour and ≤ 2 hours), `daily` (≤ 24 hours), `weekly` (≤ 7 days)
  and `monthly` (≤ 30 days).  Jobs defined in a custom SLA file are
  categorised as `custom`.
* **AI‑powered recommendations**: Optionally calls the OpenAI
  ChatCompletion API to suggest executor counts, memory and cores per
  executor.  Falls back to simple heuristics when the API key is not
  provided.
* **Slack notifications**: Sends summaries of new recommendations to a
  configured Slack channel via incoming webhooks.
* **Git integration**: Writes recommendation files into a Git
  repository and commits/pushes changes so that configuration updates
  are versioned.

## Installation

The package requires Python 3.8 or newer.  Clone the repository and
install dependencies:

```bash
pip install -r requirements.txt
```

### Dependencies

Key dependencies include:

* `requests` for HTTP calls
* `mysql‑connector‑python` for MySQL access
* `openai` (optional) for ChatCompletion API calls
* `PyYAML` (optional) for YAML configuration files
* `GitPython` or system `git` for version control (the agent uses
  ``git`` via `subprocess`)

See `requirements.txt` for the full list.

## Configuration

Configuration values can be supplied via environment variables or a
JSON/YAML configuration file.  Environment variables take precedence
over file settings.

### Single‑cluster configuration

When profiling a single Hadoop cluster, the following environment
variables (or corresponding fields in a config file) are required:

```bash
export YARN_API_URL=http://yarn-master:8088
export SPARK_HISTORY_URL=http://history-server:18080
export MYSQL_HOST=db.example.com
export MYSQL_PORT=3306
export MYSQL_USER=profiler
export MYSQL_PASSWORD=secret
export MYSQL_DATABASE=job_profile
export OPENAI_API_KEY=sk-***  # optional
export SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...  # optional
export GIT_REPO_PATH=/opt/job-config-repo  # optional
export GIT_REMOTE_URL=git@example.com:data/job-config.git  # optional
```

Alternatively create a `config.json` or `config.yaml` file with the
same keys and pass it to the CLI via `--config`.

### Custom SLA definitions

Some jobs may require bespoke service‑level agreements that differ from
the standard hourly/daily/weekly/monthly thresholds.  You can supply a
custom SLA definition file and point the agent to it via the
`CUSTOM_SLA_FILE` environment variable or the `custom_sla_file` key in
your configuration file.

The custom SLA file must be JSON or YAML and maps job identifiers
(application IDs or names) to an object with optional `category` and
`threshold_ms` properties.  When a job matches an entry in the
mapping, the specified `category` overrides the automatically computed
one.  `threshold_ms` is ignored by the analyser but may be useful for
future extensions.

Example `custom_sla.json`:

```json
{
  "application_123456789_0001": {
    "category": "special",
    "threshold_ms": 1800000
  },
  "DailyReportingJob": {
    "category": "daily"
  }
}
```

To use this file, set the environment variable:

```bash
export CUSTOM_SLA_FILE=/path/to/custom_sla.json
```

or add to your `config.yaml`:

```yaml
custom_sla_file: /path/to/custom_sla.yaml
```

Jobs not listed in the custom file will fall back to the default
threshold‑based categorisation.

### Multi‑cluster configuration

To profile multiple clusters, define a list of cluster objects in your
configuration file under the `clusters` key.  Each cluster must
specify at least:

* `name` – a unique identifier for the cluster (e.g. `LAKE`)
* `yarn_api_url` – base URL of the cluster’s YARN ResourceManager API
* `spark_history_url` – base URL of the cluster’s Spark History Server

Optional fields include `namenode_namespace` (for informational
purposes), `yarn_ui_url` and `spark_ui_url`.  Example YAML:

**Note:** If a cluster definition omits `yarn_api_url` or `spark_history_url` but
provides `yarn_ui_url` or `spark_ui_url`, the agent will automatically
use the UI URL for API calls.  This allows you to specify only UI
endpoints when the API and UI are served from the same host and port.

```yaml
mysql_host: db.example.com
mysql_port: 3306
mysql_user: profiler
mysql_password: secret
mysql_database: job_profile
openai_api_key: sk-***
slack_webhook_url: https://hooks.slack.com/services/...
git_repo_path: /opt/job-config-repo
git_remote_url: git@example.com:data/job-config.git
clusters:
  - name: LAKE
    yarn_api_url: http://lake-yarn:8088
    spark_history_url: http://lake-history:18080
    namenode_namespace: lake
    yarn_ui_url: http://lake-yarn-ui:8088
    spark_ui_url: http://lake-spark-ui:18080
  - name: STREAMING
    yarn_api_url: http://stream-yarn:8088
    spark_history_url: http://stream-history:18080
    namenode_namespace: stream
```

When using environment variables instead of a config file, you can
encode the clusters list as a JSON string in the `CLUSTERS` variable:

```bash
export CLUSTERS='[
  {"name": "LAKE", "yarn_api_url": "http://lake-yarn:8088", "spark_history_url": "http://lake-history:18080"},
  {"name": "STREAMING", "yarn_api_url": "http://stream-yarn:8088", "spark_history_url": "http://stream-history:18080"}
]'
```

If the `clusters` list is omitted, the agent falls back to the
top‑level `YARN_API_URL` and `SPARK_HISTORY_URL` fields and assigns
the cluster name `default`.

## Usage

The CLI supports four actions:

* `profile` – Collect job metadata from YARN and Spark and store it
  into the database.
* `analyse` – Read jobs from the database, compute average resource
  usage and SLA categories, and update the recommendations table (no
  tuning suggestions yet).
* `recommend` – Generate tuning recommendations for each analysed job,
  update the database, write JSON files into the Git repository (if
  configured), commit and push them, and send a summary to Slack.
* `full` – Execute the previous three actions in sequence.

Example:

```bash
python -m bigdata_ai_agent.main full --config ./config.yaml --verbose
```

When running in production, you may schedule the `full` command with
cron or Oozie to execute hourly or daily depending on your needs.

## License

This project is provided without warranty under the terms of the MIT
License.  See `LICENSE` for details.
=======
# open-stack
AI agent for profiling Spark jobs on multi‐cluster Hadoop environments.
