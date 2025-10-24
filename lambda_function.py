import boto3
import io
import datetime
from openpyxl import Workbook

# List of required tags
REQUIRED_TAGS = ["DeletionDate", "vendor", "owner", "purpose"]

# 🟩 CUSTOMIZE THESE VALUES ACCORDING TO YOUR SETUP
REGIONS = ["ap-northeast-1", "ap-south-1"]  # 🟩 Update regions as per your AWS setup
BUCKET_NAME = "vb-auto-tag-check-and-compliance-report-bucket"  # 🟩 Your destination S3 bucket
QUERY_FILTER = "-NOT (tagKey:vendor OR tagKey:owner OR tagKey:purpose OR tagKey:DeletionDate)"  # 🟩 Modify if you change tag keys

# 🟩 New helper function to fetch resource creator from CloudTrail
def get_resource_creator(resource_arn, region):
    try:
        client = boto3.client('cloudtrail', region_name=region)
        events = client.lookup_events(
            LookupAttributes=[{"AttributeKey": "ResourceName", "AttributeValue": resource_arn}],
            MaxResults=1
        )
        if events["Events"]:
            user_identity = events["Events"][0]["Username"]
            return user_identity
        else:
            return "N/A"  # 🟩 Default if no creator found
    except Exception as e:
        print(f"⚠️ Could not fetch creator for {resource_arn}: {e}")
        return "N/A"  # 🟩 Return "N/A" on any error


# Function to fetch all resources from the selected regions
def fetch_resources_from_regions():
    try:
        all_resources = []

        for region in REGIONS:  # 🟩 Now uses the variable instead of hardcoding
            print(f"🔍 Searching resources in region: {region}")
            client = boto3.client('resource-explorer-2', region_name=region)

            # Get available views
            view_response = client.list_views()
            views = view_response.get('Views', [])
            if not views:
                print(f"⚠️ No views found in region: {region}")
                continue

            first_view = views[0]
            view_arn = first_view.get('ViewArn') if isinstance(first_view, dict) else first_view
            print(f"Using ViewArn for {region}: {view_arn}")

            paginator = client.get_paginator('search')
            response_pages = paginator.paginate(QueryString=QUERY_FILTER, ViewArn=view_arn)

            for response in response_pages:
                for resource in response.get("Resources", []):
                    arn = resource.get("Arn")
                    tags = resource.get("Properties", [])
                    service = resource.get("Service", "N/A")
                    resource_type = resource.get("ResourceType", "N/A")

                    # 🟩 Get creator from CloudTrail
                    creator = get_resource_creator(arn, region)

                    all_resources.append({
                        "Arn": arn,
                        "Region": region,
                        "Service": service,
                        "ResourceType": resource_type,
                        "Creator": creator,  # 🟩 New column
                        "Tags": tags
                    })

        print(f"✅ Total resources fetched: {len(all_resources)}")
        return all_resources

    except Exception as error:
        print(f"❌ Error fetching resources: {error}")
        return []


# Function to determine tag status for each required tag
def evaluate_tag_status(resource):
    tag_status = {}
    tag_keys = {t["Data"]["Key"]: t["Data"]["Value"] for t in resource.get("Tags", []) if "Data" in t and "Key" in t["Data"]}
    
    for tag in REQUIRED_TAGS:
        if tag in tag_keys and tag_keys[tag]:
            tag_status[tag] = "Present"
        else:
            tag_status[tag] = "Missing"

    return tag_status


# Function to categorize resources by region with tag status
def categorize_by_region_with_tags(resources):
    categorized = {}
    for res in resources:
        region = res.get("Region", "unknown")
        arn = res.get("Arn")
        tag_status = evaluate_tag_status(res)
        service = res.get("Service", "N/A")
        resource_type = res.get("ResourceType", "N/A")
        creator = res.get("Creator", "N/A")  # 🟩 New field

        if region not in categorized:
            categorized[region] = []
        categorized[region].append({
            "Arn": arn,
            "Service": service,
            "ResourceType": resource_type,
            "Creator": creator,  # 🟩 Include creator column
            **tag_status
        })
    return categorized


# Function to generate Excel with multiple sheets (1 per region)
def generate_excel_report(grouped_resources):
    workbook = Workbook()
    default_sheet = workbook.active
    workbook.remove(default_sheet)

    for region, resources in grouped_resources.items():
        worksheet = workbook.create_sheet(title=region)

        # 🟩 Updated headers to include Creator
        headers = ["Resource ARN", "Service", "Resource Type", "Creator"] + REQUIRED_TAGS  # 🟩
        worksheet.append(headers)

        for res in resources:
            # 🟩 Updated row to include Creator
            row = [res["Arn"], res["Service"], res["ResourceType"], res["Creator"]] + [res[tag] for tag in REQUIRED_TAGS]  # 🟩
            worksheet.append(row)

        # Adjust column width
        for column_cells in worksheet.columns:
            length = max(len(str(cell.value)) for cell in column_cells)
            worksheet.column_dimensions[column_cells[0].column_letter].width = min(length + 2, 80)

    excel_buffer = io.BytesIO()
    workbook.save(excel_buffer)
    excel_buffer.seek(0)
    return excel_buffer


# Function to upload Excel file to S3
def upload_excel_to_s3(excel_buffer, bucket_name, file_name):
    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=excel_buffer.getvalue(),
            ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        print(f"✅ Report uploaded to S3: {file_name}")
    except Exception as error:
        print(f"❌ Failed to upload report: {error}")


# Lambda handler
def lambda_handler(event, context):
    try:
        print("🚀 Execution started...")

        resources = fetch_resources_from_regions()
        if not resources:
            print("No resources found.")
            return

        categorized = categorize_by_region_with_tags(resources)
        excel_report = generate_excel_report(categorized)

        # 🟩 Using variable instead of hardcoded string
        current_time = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        file_name = f"tag-compliance-report-{current_time}.xlsx"

        upload_excel_to_s3(excel_report, BUCKET_NAME, file_name)  # 🟩
        print(f"✅ Excel report generated and uploaded successfully: {file_name}")

    except Exception as e:
        print(f"❌ Error during Lambda execution: {e}")
