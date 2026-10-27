# Scheduled Parquet batch pipeline

## Project summary

A serverless AWS ingestion pipeline polls a raw S3 prefix every two hours and processes up to 300 new files per batch. Lambda validates and routes files: Glue transforms valid Parquet into curated S3, while rejected files go to quarantine. Step Functions starts the next batch immediately and stops when no files remain; DynamoDB tracks processing state, SNS sends run summaries, and CloudWatch monitors the workflow.

**Git repository description:** Scheduled AWS pipeline that validates raw S3 files in 300-file batches, transforms Parquet with Glue, quarantines rejects, and reports run status.

## Deploy

Prerequisites: AWS CLI, SAM CLI, Docker, permissions to deploy the resources in `template.yaml`, and existing raw/quarantine/curated S3 buckets.

1. Review `glue/transform.py` and replace its example transformation with your schema and business rules.
2. Upload `glue/transform.py` to an artifacts bucket and provide its `s3://...` URI as `GlueScriptUri`.
3. Run `sam build` then `sam deploy --guided`. Enter your bucket names and prefixes at deployment time. No bucket names, account IDs, ARNs, credentials, or secrets are embedded in this repo.
4. Optionally provide an email for the SNS summary subscription and confirm the subscription.
5. Start a manual Step Functions execution to validate the deployment. Schedule is `rate(2 hours)` (UTC-based).

## Layout

- `template.yaml`: SAM resources, IAM, Step Functions, schedule, Glue and CloudWatch.
- `statemachine/pipeline.asl.json`: drain-the-queue workflow.
- `src/`: discovery/claim, quarantine, completion, SNS summary Lambdas.
- `glue/transform.py`: Glue Spark job starter.

## Operational notes

The lightweight Parquet check verifies `.parquet` extension and `PAR1` header/footer; Glue performs the full read. A file with valid markers and damaged internals can still fail the Glue job. Failed claims become eligible again after `ClaimTimeoutMinutes`. The manifest uses bucket + key; enable versioning and include version ID if keys can be overwritten. Prefix enumeration is suitable for moderate volumes; use S3 notifications plus SQS at very large scale. Configure encryption, retention, lifecycle, KMS, alert routing, and retry policies for your account standards. Never commit credentials or production data.


## Architecture diagram

`workflow-architecture.svg` is a self-contained vector diagram using AWS Architecture Icons from the [official icon page](https://aws.amazon.com/architecture/icons/).
