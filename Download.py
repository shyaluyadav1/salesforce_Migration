#This script extracts all data from salesforce and include pagination and multiprocessing 
import concurrent.futures
import requests
from simple_salesforce import Salesforce, SalesforceMalformedRequest
import os

# Salesforce credentials
username = 'abcy'

password = 'abc'

security_token = 'abcScl'

# Specify the directory where you want to save the attachments
download_path = 'your path'  # Change this to your desired path

# Ensure the download directory exists
if not os.path.exists(download_path):
    os.makedirs(download_path)

# Salesforce connection instance
sf = Salesforce(username=username, password=password, security_token=security_token)

# Example query (modify as needed)
attachments_query = """ your soql query  """



# Function to download a single attachment
def download_attachment(attachment, sf, download_path):
    attachment_id = attachment['Id']
    attachment_body_url = f'https://{sf.sf_instance}/services/data/v62.0/sobjects/Attachment/{attachment_id}/Body'

    # Make a request to download the file
    response = requests.get(attachment_body_url, headers={'Authorization': f'Bearer {sf.session_id}'})

    if response.ok:
        # Create the full file path
        file_path = os.path.join(download_path, attachment['ParentId'] + '_' + attachment['Id'] + '_Email Outbound_' + attachment['Name'])
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded: {file_path}")
    else:
        print(f"Failed to download attachment {attachment['Name']}: {response.text}")

# Function to fetch all attachments with pagination
def fetch_all_attachments(sf, attachments_query):
    attachments = []
    query_url = f'https://{sf.sf_instance}/services/data/v62.0/query?q={attachments_query}'
    
    while query_url:
        response = requests.get(query_url, headers={'Authorization': f'Bearer {sf.session_id}'})
        
        if response.ok:
            data = response.json()
            attachments.extend(data.get('records', []))  # Add records to the list
            
            # Check if there is a next page
            next_url = data.get('nextRecordsUrl')
            if next_url:
                # Prepend the base URL to the relative nextRecordsUrl to get the next page URL
                query_url = f'https://{sf.sf_instance}{next_url}'
            else:
                # No more pages, stop pagination
                query_url = None
        else:
            print(f"Error fetching attachments: {response.text}")
            break
    
    return attachments

# Main function to download all attachments
def download_all_attachments(sf, attachments_query, download_path):
    try:
        # Fetch all attachments using pagination
        attachments = fetch_all_attachments(sf, attachments_query)

        # Use ThreadPoolExecutor to download files concurrently
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Map download_attachment function to each attachment in attachments['records']
            executor.map(lambda attachment: download_attachment(attachment, sf, download_path), attachments)
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")

# Download all attachments
download_all_attachments(sf, attachments_query, download_path)
