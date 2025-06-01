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
    'group.id': 'hello-service-group',
    'auto.offset.reset': 'earliest'
}

FROM_TOPIC = "server_request_topic"
TO_TOPIC = "server_response_topic"

consumer = Consumer(conf)
producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

tools = []

# --- Configuration ---
ALLOWED_FILE_DIRECTORY = './' # IMPORTANT: Security!

######### Tools Start ###########

def write_to_file(filename:str, text:str) -> str:
    os.makedirs(ALLOWED_FILE_DIRECTORY, exist_ok=True) 
    file_path = os.path.join(ALLOWED_FILE_DIRECTORY, filename)
    with open(file_path,"w") as file:
        file.write(text)
    response = f"File {filename} written successfully"
    return filename

def send_to_whatsapp(phonenumber:str, message:str="Hi") -> str:
    try:
        pywhatkit.sendwhatmsg_instantly(phonenumber, 
                        message, 10, tab_close=True)
        return "Whatsapp message sent successfully"
    except:
        print("An Unexpected Error!")

    




def register_tool():
    writer_tool = {
        "name": "write_to_file",
        "description": "Write text data to a specified file in allowed directory",
        "inputSchema": {
          "type": "object",
          "properties": {
            "filename": {
              "type": "string",
              "description": "Filename to be created"
            },
            "text":{
              "type": "string",
              "description": "Text to be written to the file" 
            }
          },
          "required": ["filename","text"]
        }
    }

    whatsapp_tool = {
        "name": "send_to_whatsapp",
        "description": "Send message to whatsapp on the provided phone number",
        "inputSchema": {
          "type": "object",
          "properties": {
            "phonenumber": {
              "type": "string",
              "description": "Full phone number including isd code in +x format"
            },
            "message":{
              "type": "string",
              "description": "Message to be sent to whatsapp" 
            }
          },
          "required": ["phonenumber"]
        }
    }

    tools.append(writer_tool)
    tools.append(whatsapp_tool)
    
function_map = {
    "write_to_file": write_to_file,
    "send_to_whatsapp": send_to_whatsapp
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

def list_available_files():
    # SECURITY: Only list files from the ALLOWED_FILE_DIRECTORY
    # and perform proper path validation to prevent directory traversal.
    files = []
    for item in os.listdir(ALLOWED_FILE_DIRECTORY):
        item_path = os.path.join(ALLOWED_FILE_DIRECTORY, item)
        if os.path.isfile(item_path):
            files.append({
                "uri": f"file://{os.path.abspath(item_path)}", # MCP uses URIs
                "name": item,
                # Add other metadata if needed (e.g., size, type)
            })
    return files

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
    
    
    


def handle_list_resources(request_id, params):
    # This method would list files as resources
    resources = list_available_files()
    send_response(request_id, result={"resources": resources})

def handle_read_resource(request_id, params):
    uri = params.get("uri")
    if not uri or not uri.startswith("file://"):
        send_response(request_id, error={"code": -32602, "message": "Invalid params: URI missing or not a file URI"})
        return

    file_path = uri[7:] # Strip "file://"
    
    # CRITICAL SECURITY CHECK: Ensure the path is within ALLOWED_FILE_DIRECTORY
    # and resolve any symlinks or relative paths safely (e.g., os.path.realpath)
    # to prevent directory traversal attacks.
    abs_file_path = os.path.abspath(file_path)
    abs_allowed_dir = os.path.abspath(ALLOWED_FILE_DIRECTORY)

    if not abs_file_path.startswith(abs_allowed_dir):
        send_response(request_id, error={"code": -32000, "message": "Access denied: File is outside allowed directory"})
        return

    try:
        with open(abs_file_path, 'r', encoding='utf-8') as f: # Or 'rb' for binary
            content = f.read()
        send_response(request_id, result={"content": content, "mimeType": "text/plain"}) # Adjust mimeType as needed
    except FileNotFoundError:
        send_response(request_id, error={"code": -32001, "message": "File not found"})
    except Exception as e:
        send_response(request_id, error={"code": -32002, "message": f"Error reading file: {str(e)}"})

running = True

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

    '''
    try:
        consumer.subscribe([FROM_TOPIC])

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: 
                continue

            if msg.error():
                continue
            else:
                msg_process(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
        running = False
    '''

if __name__ == "__main__":
    # Setup logging, etc.
    main_loop()