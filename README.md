## Projects

1) `etl` - Main project that contains a session enrichment logic
2) `etl-local` - Auxiliary project to run jobs with Spark Standalone Mode
3) `data-generator` - Project to generate input logs for `SessionLogsEnrichmentJob`

## Main parts

* `me.vitaly.etl.jobs.SessionLogsEnrichmentJob` - the main job which takes raw logs and previously handled session logs
  as input and saves new session logs in partitioned by data, month, year directories.
* `me.vitaly.etl.runners.SessionLogsEnrichmentJobRunner` - the runner of `SessionLogsEnrichmentJob` which:
    1) Makes validations that input logs has not been already processed.
    2) Calculates files to process based on configs and input parameters
    3) Mark files as processed after the job is finished.
* `me.vitaly.etl.jobs.SessionLogsEnrichmentJobTest` - parameterized unit tests to check common and edge cases.
* `etl/src/main/resources/application.conf` - file with configurations

## How to run locally

* Run `me.vitaly.etl.generator.DataGeneratorKt.main` from the `data-generator` to generate logs to `data/raw/year=2021/month=04/day=14`.
  Note that is a ~~stupid~~ naive generator without any session logic.
* Run `me.vitaly.etl.local.SessionLogsLocalRunnerKt.main` from the `etl-local`. 
  It runs`SessionLogsEnrichmentJob`  using config `etl/src/main/resources/application.conf` and the data from the `data` folder on Spark Standalone.
  The results should be saved to `data/processed/year=2021/month=04/day=13`.
  