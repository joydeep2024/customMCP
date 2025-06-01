# Conceptual Python-like pseudocode

import json
import sys
import os

from confluent_kafka import Producer,Consumer
import socket
import kafka_transport

# importing the module
import pywhatkit

BOOTSTRAP_SERVERS = 'hostname:9092'
server_transport = None

conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': 'hello-data-group',
    'auto.offset.reset': 'earliest'
}

FROM_TOPIC = "datastore_request_topic"
TO_TOPIC = "datastore_response_topic"

consumer = Consumer(conf)
producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

tools = []

contact_name_number = {
    "Name1": "+91XXXXXXXXXX",
    "Name2": "+91XXXXXXXXXX"
}


######### Tools Start ###########

def fetch_contact_phonenumber(contactname:str) -> str:
    if contactname in contact_name_number:
        phone_number = contact_name_number[contactname]
        return phone_number
    else:
        return ""
   




def register_tool():
    contactnumber_tool = {
        "name": "fetch_contact_phonenumber",
        "description": "Find the phone number of a person in my contacts from his or her name.",
        "inputSchema": {
          "type": "object",
          "properties": {
            "contactname": {
              "type": "string",
              "description": "Name of the contact whose phone number needs to be fetched"
            }
          },
          "required": ["contactname"]
        }
    }

    tools.append(contactnumber_tool)
    
function_map = {
    "fetch_contact_phonenumber": fetch_contact_phonenumber
}
    

######### Tools End #############








def send_response(request_id, result=None, error=None):
    message = {"jsonrpc": "2.0", "id": request_id}
    if error:
        message["error"] = error
    else:
        message["result"] = result
    
    print(f"Sending response: {message}")
    server_transport._kafka_write(message)

def handle_initialize(request_id, params):
    # Respond with server capabilities
    resultJson = {
        "protocolVersion": "2024-11-05", # Example version
        "serverInfo": {
            "name": "LowLevelFileServer",
            "version": "0.0.1"
        },
        "capabilities": {
            "logging": {},
            "tools": {
                "listChanged": True
            }
        }
    } 
    send_response(request_id, result=resultJson)

def handle_list_tools(request_id, params):
    # This method would list files as resources
    global tools
    response_tools = tools
    send_response(request_id, result={"tools": response_tools})

def handle_tool_call(request_id, params):
    data = params
    func_name = data["name"]
    args = data["arguments"]
    result = {}
    isError = True
    if func_name in function_map:
        result_text = function_map[func_name](**args)
        isError = False
        if(result_text == ""):
            isError = True
        print(result_text)
        result = {
            "content": [
                {
                    "type": "text",
                    "text": result_text
                }
            ],
            "isError": isError
        }
        send_response(request_id, result=result)
    else:
        error_text = f"Function '{func_name}' not found."
        print(error_text)
    

def msg_process(msg):
    print(f"Received message: {msg}")
    line = msg
    #print(line)
    #content_length = int(line.split(":")[1].strip())
    #print(content_length)

    # Read the JSON content
    raw_request = line
    if not raw_request:
        return None # End of input

    try:
        request = raw_request
        request_id = request.get("id")
        method = request.get("method")
        params = request.get("params", {})

        print("Request method " + method)

        if method == "initialize":
            handle_initialize(request_id, params)
        elif method == "tools/list":
            handle_list_tools(request_id, params)
        elif method == "tools/call":
            handle_tool_call(request_id, params)
        elif method == "mcp/listResources": # Assuming files are resources
            handle_list_resources(request_id, params)
        elif method == "mcp/readResource":
            handle_read_resource(request_id, params)
        # Add mcp/shutdown, mcp/exit, and other MCP methods as needed
        else:
            if request_id is not None: # It's not a notification
                send_response(request_id, error={"code": -32601, "message": "Method not found"})
    
    except json.JSONDecodeError:
        # Send parse error if it's not a notification that failed to parse
        # This requires knowing if an ID was part of the garbled message, which is tricky.
        # Often, servers might just log and ignore parse errors for robustness.
        # If an ID can be extracted, send:
        # send_response(None, error={"code": -32700, "message": "Parse error"}) 
        pass # Or log
    except Exception as e:
        # Generic error handling
        if request_id: send_response(request_id, error={"code": -32000, "message": f"Server error: {str(e)}"})
        pass # Or log
    

def main_loop():
    running = True
    register_tool()
    global server_transport
    server_transport = kafka_transport.kafka_transport(BOOTSTRAP_SERVERS,FROM_TOPIC,TO_TOPIC)

    try:
        while running:
            response = server_transport._kafka_read()
            msg_process(response)
    finally:
        running = False

if __name__ == "__main__":
    # Setup logging, etc.
    main_loop()