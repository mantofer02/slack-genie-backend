import os
from flask import Flask, request, jsonify
import requests
import time
import json
from slack_sdk import WebClient
from slack_sdk.signature import SignatureVerifier

# Load environment variables

app = Flask(__name__)

# Configuration
DATABRICKS_HOST = ''
DATABRICKS_TOKEN = ''
GENIE_SPACE_ID = ''
SLACK_SIGNING_SECRET = ''
#SLACK_BOT_TOKEN = ''
SLACK_BOT_TOKEN = ''

# Initialize Slack client
slack_client = WebClient(token=SLACK_BOT_TOKEN)
signature_verifier = SignatureVerifier(SLACK_SIGNING_SECRET)

@app.route('/slack/commands', methods=['POST'])
def slack_commands():
    """
    Handle Slack slash commands
    """
    # Verify the request came from Slack
    if not verify_slack_request(request):
        return "Unauthorized", 401
    
    # Extract command data
    command = request.form.get('command')
    text = request.form.get('text')
    channel_id = request.form.get('channel_id')
    user_id = request.form.get('user_id')
    
    if command == '/genie':
        # Process the query with Genie API
        try:
            # Show immediate response to user
            slack_client.chat_postMessage(
                channel=channel_id,
                text=f"Processing your query: {text}..."
            )
            
            # Call Genie API
            genie_response = call_genie_api(text)
            
            # Send response back to Slack
            slack_client.chat_postMessage(
                channel=channel_id,
                text=genie_response
            )
            
            # Return empty 200 response to Slack
            return "", 200
            
        except Exception as e:
            error_message = f"Sorry, there was an error processing your request: {str(e)}"
            print(error_message)
            
            # Send error message to Slack
            slack_client.chat_postMessage(
                channel=channel_id,
                text=error_message
            )
            
            # Return empty 200 response to Slack
            return "", 200
    
    return "Unsupported command", 400

def verify_slack_request(request):
    """
    Verify that the request came from Slack
    """
    try:
        signature = request.headers.get('X-Slack-Signature', '')
        timestamp = request.headers.get('X-Slack-Request-Timestamp', '')
        body = request.get_data()
        
        return signature_verifier.is_valid(
            body=body,
            timestamp=timestamp,
            signature=signature
        )
    except Exception as e:
        print(f"Error verifying Slack request: {e}")
        return False

def call_genie_api(message):
    """
    Call the Databricks Genie API with the given message
    """
    try:
        # Step 1: Start a conversation
        conversation_response = start_genie_conversation(message)
        
        if not conversation_response:
            return "Error: Could not start conversation with Genie"
        
        conversation_id = conversation_response.get('conversation', {}).get('id')
        message_id = conversation_response.get('message', {}).get('id')
        
        if not conversation_id or not message_id:
            return "Error: Invalid response from Genie API"
        
        # Step 2: Poll for the response
        message_data = poll_genie_message(conversation_id, message_id)
        
        # Step 3: Process the response
        status = message_data.get('status')
        
        if status == 'COMPLETED':
            # Check if there are attachments (query results)
            attachments = message_data.get('attachments', [])
            if attachments:
                # Get the query result using executeMessageAttachmentQuery
                attachment_id = attachments[0].get('id')
                return execute_message_attachment_query(conversation_id, message_id, attachment_id)
            else:
                # Return the text response
                return message_data.get('content', 'No response content available')
        
        elif status == 'FAILED':
            error = message_data.get('error', {})
            return f"Genie query failed: {error.get('message', 'Unknown error')}"
        
        else:
            return f"Unexpected status: {status}"
        
    except Exception as e:
        print(f"Error calling Genie API: {e}")
        return f"Error: {str(e)}"

def start_genie_conversation(message):
    """
    Start a new conversation with Genie
    """
    url = f"https://{DATABRICKS_HOST}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "content": message
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error starting Genie conversation: {e}")
        return None

def poll_genie_message(conversation_id, message_id, max_attempts=30, delay=2):
    """
    Poll for the message status from Genie
    """
    url = f"https://{DATABRICKS_HOST}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages/{message_id}"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }
    
    for attempt in range(max_attempts):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            message_data = response.json()
            
            status = message_data.get('status')
            
            # Handle completed or failed states
            if status in ['COMPLETED', 'FAILED', 'CANCELLED']:
                return message_data
            
            # Handle in-progress states (including the new FILTERING_CONTEXT status)
            elif status in ['IN_PROGRESS', 'PENDING', 'FILTERING_CONTEXT', 'EXECUTING_QUERY', 'ASKING_AI', 'PENDING_WAREHOUSE']:
                # Continue polling
                time.sleep(delay)
                continue
            
            else:
                print(f"Unknown status: {status}")
                return message_data
                
        except requests.exceptions.RequestException as e:
            print(f"Error polling Genie message (attempt {attempt + 1}): {e}")
            if attempt == max_attempts - 1:
                raise Exception(f"Failed to get response after {max_attempts} attempts")
            time.sleep(delay)
    
    raise Exception("Timeout waiting for Genie response")


def execute_message_attachment_query(conversation_id, message_id, attachment_id):
    """
    Execute the query for a message attachment using the correct query-result endpoint
    """
    # Corrected URL - using query-result instead of execute-attachment-query
    url = f"https://{DATABRICKS_HOST}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/query-result"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    try:
        # Use GET request instead of POST for query-result endpoint
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        result_data = response.json()
        
        # Format the result for Slack
        return format_query_result(result_data)
        
    except requests.exceptions.RequestException as e:
        print(f"Error executing message attachment query: {e}")
        return f"Error retrieving query result: {str(e)}"


def format_query_result(result_data):
    """
    Format the Genie query result for display in Slack
    """
    try:
        # Get the SQL query if available
        sql_query = result_data.get('statement_response', {}).get('manifest', {}).get('schema', {}).get('sql')
        
        # Get the result data
        result = result_data.get('result', {})
        data_array = result.get('data_array', [])
        
        if not data_array:
            return "No data returned from the query"
        
        # Format as a simple table for Slack
        formatted_result = ""
        
        # Add SQL query if available
        if sql_query:
            formatted_result += f"*SQL Query:*\n``````\n\n"
        
        formatted_result += "*Results:*\n```"
        
        # Add rows (limit to 10 for Slack)
        for i, row in enumerate(data_array[:10]):
            formatted_result += f"Row {i+1}: {row}\n"
        
        if len(data_array) > 10:
            formatted_result += f"\n... and {len(data_array) - 10} more rows"
        
        formatted_result += "```"
        
        return formatted_result
        
    except Exception as e:
        print(f"Error formatting query result: {e}")
        return f"Query completed but failed to format result: {str(e)}"

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
