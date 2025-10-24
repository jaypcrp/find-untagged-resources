import boto3
import io
import datetime
from openpyxl import Workbook

# Function to fetch ARNs of resources that are missing a specific tag key
def fetch_resource_arns():
    try:
        # Only target these two regions
        regions = ["ap-northeast-1", "ap-south-1"]
        query_filter = '-tag.key:vendor'
        resource_arns = []

        for region in regions:
            print(f"🔍 Searching resources in region: {region}")
            client = boto3.client('resource-explorer-2', region_name=region)

            try:
                # Get the available views in this region
                view_response = client.list_views()
                if not view_response.get('Views'):
                    print(f"⚠️ No views found in region: {region}")
                    continue

                # Use the first (or default) view ARN
                view_arn = view_response['Views'][0]['ViewArn']
                print(f"Using ViewArn for {region}: {view_arn}")

                # Paginate through search results (max 1000 per region)
                paginator = client.get_paginator('search')
                response_pages = paginator.paginate(
                    QueryString=query_filter,
                    ViewArn=view_arn
                )

                for response in response_pages:
                    for resource in response.get('Resources', []):
                        resource_arns.append(resource['Arn'])

            except Exception as e:
                print(f"❌ Error while fetching from region {region}: {e}")
        # Deduplicate ARNs
        unique_arns = list(set(resource_arns))
        print(f"✅ Total untagged resources found: {len(unique_arns)}")
        return unique_arns
    except Exception as error:
        print(f"Failed to retrieve resource ARNs: {error}")
        return []

# Function to categorize resources by their region
def categorize_resources_by_region(resource_arns):
    try:
        regional_resources = {}
        for arn in resource_arns:
            if ':' in arn:
                region = arn.split(':')[3]
                if region not in regional_resources:
                    regional_resources[region] = []
                regional_resources[region].append(arn)
        return regional_resources
    except Exception as error:
        print(f"Error while grouping resources by region: {error}")
        return {}


# Function to generate Excel report with multiple region tabs
def generate_excel_report(grouped_resources):
    workbook = Workbook()
    default_sheet = workbook.active
    workbook.remove(default_sheet)  # remove default blank sheet

    for region, arns in grouped_resources.items():
        worksheet = workbook.create_sheet(title=region)
        worksheet.append(["Resource ARN", "Status"])
        for arn in arns:
            worksheet.append([arn, "Untagged"])

        # Auto-adjust column widths for neatness
        for column_cells in worksheet.columns:
            length = max(len(str(cell.value)) for cell in column_cells)
            worksheet.column_dimensions[column_cells[0].column_letter].width = min(length + 2, 80)

    # Save workbook to an in-memory buffer
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
        print(f"Report uploaded to S3: {file_name}")
    except Exception as error:
        print(f"Failed to upload report to S3: {error}")


# Main function for AWS Lambda
def lambda_handler(event, context):
    try:
        print("Execution started...")

        # Fetch untagged resources
        resources = fetch_resource_arns()
        if resources:
            print(f"Total untagged resources found: {len(resources)}")
            print("Grouping resources by region...")

            # Group by region
            grouped_resources = categorize_resources_by_region(resources)

            # Generate Excel report with multiple sheets
            excel_report = generate_excel_report(grouped_resources)

            # Define S3 bucket and file name
            bucket_name = "vb-auto-tag-check-and-compliance-report-bucket"
            current_time = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            file_name = f"untagged-resources-report-{current_time}.xlsx"

            # Upload report to S3
            upload_excel_to_s3(excel_report, bucket_name, file_name)

            print(f"Excel report successfully uploaded: {file_name}")
        else:
            print("No untagged resources found.")
    except Exception as error:
        print(f"Error during Lambda execution: {error}")
