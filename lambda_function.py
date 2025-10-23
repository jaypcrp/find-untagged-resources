import boto3
import pandas as pd
import os
from datetime import datetime, timedelta, timezone
import json

# ========= CONFIGURATION =========
REQUIRED_TAG_KEYS = os.getenv("REQUIRED_TAG_KEYS", "Owner,Environment").split(",")
REGIONS = os.getenv("REGIONS", "ap-south-1").split(",")
DAYS_LOOKBACK = int(os.getenv("DAYS_LOOKBACK", "30"))
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/tmp/")
S3_BUCKET = os.getenv("S3_BUCKET", "")
# =================================


def upload_to_s3(local_path, bucket, prefix="untagged-reports/"):
    """Upload generated file to S3."""
    s3 = boto3.client("s3")
    file_name = os.path.basename(local_path)
    s3_key = f"{prefix}{file_name}"
    s3.upload_file(local_path, bucket, s3_key)
    print(f"✅ Uploaded {file_name} to s3://{bucket}/{s3_key}")


def get_untagged_resources_resource_explorer(region):
    """Use Resource Explorer 2 to get all resources missing the 'vendor' tag in a region."""
    rex = boto3.client("resource-explorer-2", region_name=region)
    untagged = []
    paginator = rex.get_paginator("search")
    # We want resources that do NOT have key=vendor, in this region
    query_string = "-tag.key:vendor region:{}".format(region)

    try:
        for page in paginator.paginate(QueryString=query_string, MaxResults=100):
            for res in page.get("Resources", []):
                arn = res["Arn"]
                untagged.append({
                    "ResourceARN": arn,
                    "Service": arn.split(":")[2],
                    "Region": region,
                    "MissingTags": "vendor"
                })
            # continue automatically until NextToken is exhausted
        return untagged

    except rex.exceptions.ValidationException as e:
        # if your view isn’t indexed for vendor, ValidationException will tell you
        print(f"⚠️ Resource Explorer validation failed in {region}: {e}")
        return []
    except rex.exceptions.AccessDeniedException:
        print(f"⚠️ Resource Explorer not enabled in {region}. Falling back to Tagging API.")
        return []
    except Exception as e:
        print(f"❌ Resource Explorer failed in {region}: {e}")
        return []



def get_untagged_resources_tagging_api(region):
    """Fallback: Use Resource Groups Tagging API."""
    untagged = []
    tag_client = boto3.client("resourcegroupstaggingapi", region_name=region)
    paginator = tag_client.get_paginator("get_resources")

    for page in paginator.paginate(ResourcesPerPage=50):
        for resource in page.get("ResourceTagMappingList", []):
            arn = resource["ResourceARN"]
            tags = {t["Key"]: t["Value"] for t in resource.get("Tags", [])}
            missing = [key for key in REQUIRED_TAG_KEYS if key not in tags]
            if missing:
                untagged.append({
                    "ResourceARN": arn,
                    "Service": arn.split(":")[2],
                    "Region": region,
                    "MissingTags": ", ".join(missing)
                })
    return untagged


def get_creator_from_cloudtrail(arn, region, start_time, end_time):
    """Find who created the given resource ARN from CloudTrail logs."""
    cloudtrail = boto3.client("cloudtrail", region_name=region)
    try:
        events = cloudtrail.lookup_events(
            LookupAttributes=[{"AttributeKey": "ResourceName", "AttributeValue": arn.split("/")[-1]}],
            StartTime=start_time,
            EndTime=end_time,
            MaxResults=5
        )
        if events.get("Events"):
            event = events["Events"][0]
            username = event.get("Username", "Unknown")
            event_name = event.get("EventName", "Unknown")
            event_time = event.get("EventTime", "Unknown")
            return username, event_name, event_time
    except Exception:
        pass
    return "Unknown", "Unknown", "Unknown"


def main():
    print("🔍 Scanning for untagged resources across regions...")
    all_untagged = []

    for region in REGIONS:
        region = region.strip()
        if not region:
            continue

        print(f"➡️ Checking region: {region}")
        untagged = get_untagged_resources_resource_explorer(region)

        # fallback if RE not enabled
        if not untagged:
            untagged = get_untagged_resources_tagging_api(region)

        all_untagged.extend(untagged)

    print(f"Found {len(all_untagged)} untagged resources total")

    # Save untagged resources
    untagged_df = pd.DataFrame(all_untagged)
    untagged_file = f"{OUTPUT_DIR}untagged_resources_{datetime.now().strftime('%Y%m%d')}.xlsx"
    untagged_df.to_excel(untagged_file, index=False)
    print(f"✅ Untagged resources report saved: {untagged_file}")

    # CloudTrail creator lookup
    print("🔍 Fetching creators from CloudTrail (this may take time)...")
    start_time = datetime.now(timezone.utc) - timedelta(days=DAYS_LOOKBACK)
    end_time = datetime.now(timezone.utc)
    created_records = []

    for res in all_untagged:
        arn = res["ResourceARN"]
        service = res["Service"]
        arn_parts = arn.split(":")
        region = arn_parts[3] if len(arn_parts) > 3 and arn_parts[3] else REGIONS[0]

        username, event_name, event_time = get_creator_from_cloudtrail(arn, region, start_time, end_time)
        created_records.append({
            "ResourceARN": arn,
            "Service": service,
            "CreatedBy": username,
            "EventName": event_name,
            "EventTime": event_time,
            "MissingTags": res["MissingTags"]
        })

    creator_df = pd.DataFrame(created_records)
    if "EventTime" in creator_df.columns:
        creator_df["EventTime"] = pd.to_datetime(creator_df["EventTime"], errors="coerce").dt.tz_localize(None)

    creator_file = f"{OUTPUT_DIR}untagged_resource_creators_{datetime.now().strftime('%Y%m%d')}.xlsx"
    creator_df.to_excel(creator_file, index=False)
    print(f"✅ Creator details report saved: {creator_file}")

    # Upload to S3 if configured
    if S3_BUCKET:
        print("☁️ Uploading reports to S3...")
        upload_to_s3(untagged_file, S3_BUCKET)
        upload_to_s3(creator_file, S3_BUCKET)
    else:
        print("⚠️ No S3_BUCKET environment variable found. Skipping upload.")

    print("🎯 Completed successfully.")


def lambda_handler(event, context):
    main()
    return {"status": "success", "message": "Tag compliance check completed"}