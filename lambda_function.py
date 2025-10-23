import boto3
import csv
import io
import datetime

# Function to fetch ARNs of resources that are missing a specific tag key
def fetch_resource_arns():
    try:
        client = boto3.client('resource-explorer-2')
        
        default_view_arn = "arn:aws:resource-explorer-2"
        query_filter = '-tag.key:ENV'
        
        resource_arns = []
        paginator = client.get_paginator('search')
        response_pages = paginator.paginate(
            QueryString=query_filter,
            ViewArn=default_view_arn
        )
        
        for response in response_pages:
            resource_items = response['Resources']
            for resource in resource_items:
                resource_arns.append(resource['Arn'])
        
        return list(set(resource_arns))
    except Exception as error:
        print(f"Failed to retrieve resource ARNs: {error}")

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

# Function to generate CSV report for untagged resources
def generate_csv_report(untagged):
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    
    # Write header
    csv_writer.writerow(["Resource ARN", "Status"])
    
    # Write untagged resources
    for arn in untagged:
        csv_writer.writerow([arn, "Untagged"])
    
    return csv_buffer.getvalue()

# Function to upload CSV report to S3
def upload_csv_to_s3(csv_data, bucket_name, file_name):
    s3_client = boto3.client('s3')
    try:
        # Upload CSV file to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=csv_data,
            ContentType='text/csv'
        )
        print(f"Report uploaded to S3: {file_name}")
    except Exception as error:
        print(f"Failed to upload report to S3: {error}")

# Main function for the AWS Lambda handler
def lambda_handler(event, context):
    try:
        print("Execution started...")
        
        # Fetch the list of resources missing the specific tag
        resources = fetch_resource_arns()
        if resources:
            print("Grouping resources by region...")
            
            # Group resources by region
            grouped_resources = categorize_resources_by_region(resources)
            
            # Flatten all grouped resource ARNs (since we’re not tagging)
            all_untagged_resources = [arn for region_arns in grouped_resources.values() for arn in region_arns]
            
            # Generate the CSV report
            csv_report = generate_csv_report(all_untagged_resources)
            
            # Define the S3 bucket and file name
            bucket_name = "S3-BUCKET-NAME"
            current_time = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            file_name = f"untagged-resources-report-{current_time}.csv"
            
            # Upload the report to S3
            upload_csv_to_s3(csv_report, bucket_name, file_name)
            
            print(f"Number of untagged resources: {len(all_untagged_resources)}")
        else:
            print("No untagged resources found.")
    except Exception as error:
        print(f"Error during lambda execution: {error}")
