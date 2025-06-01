# mcp_client.py
import json
import subprocess
import sys
import os
import time
import uuid



from confluent_kafka import Producer,Consumer
import socket
import threading
import kafka_transport

from anthropic import Anthropic

BOOTSTRAP_SERVERS = 'hostname:9092'
ANTHROPIC_API_KEY=os.getenv("ANTHROPIC_API_KEY")


conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': "hello-service-group",  # unique group ID for each client
        'auto.offset.reset': 'earliest'
    }

FROM_TOPIC = "server_response_topic"
TO_TOPIC = "server_request_topic"

consumer = Consumer(conf)
producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

response_received = threading.Event()
response_data = None
request_id_counter = -1


class MCPClient:
    def __init__(self, server_config):
        from_topic = server_config["fromTopic"]
        to_topic = server_config["toTopic"]
        self.process = None
        self.request_id_counter = 0
        self.server_transport = kafka_transport.kafka_transport(BOOTSTRAP_SERVERS,from_topic,to_topic)
        self.initialized = False

        self.hasTools = False
        self.hasResources = False
        self.hasPrompts = False

        self.tools = []
        self.resources = []
        self.prompts = []

    def _start_server(self):
        if self.process and self.process.poll() is None:
            print("Server already running.")
            return
        try:
            # Ensure the server script has execute permissions or is run via python
            self.process = subprocess.Popen(
                [PYTHON_EXECUTABLE, self.server_command],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, # Capture stderr for debugging server issues
                bufsize=0 # Unbuffered
            )
            print(f"Started server with PID: {self.process.pid}")
            # Give the server a moment to start up
            time.sleep(0.5)
            if self.process.poll() is not None:
                # Server terminated prematurely
                stderr = self.process.stderr.read().decode('utf-8', errors='ignore')
                raise ConnectionError(f"Server process terminated unexpectedly. Stderr:\n{stderr}")
        except FileNotFoundError:
            print(f"Error: Server script '{self.server_command}' not found or python executable '{PYTHON_EXECUTABLE}' is incorrect.")
            raise
        except Exception as e:
            print(f"Error starting server: {e}")
            raise


    def _send_request(self, method, params=None,clientinfo=None):
        
        self.request_id_counter += 1
        request = {
            "jsonrpc": "2.0",
            "id": self.request_id_counter,
            "method": method,
        }
        if params is not None:
            request["params"] = params
        if clientinfo is not None:
            request["clientInfo"] = clientinfo

        #serialized_request = json.dumps(request)
        #message = f"Content-Length: {len(serialized_request)}\r\n\r\n" + serialized_request
        #message = serialized_request
        print(f"Sending request: {request}")
        self.server_transport._kafka_write(request)

    def _send_notification(self, method):
        notification = {
            "jsonrpc": "2.0",
            "method": method
        }
        self.server_transport._kafka_write(notification)


    def _receive_response(self):
        
        # Wait for response
        print("Waiting for response...")
        response_data = self.server_transport._kafka_read()
        print(f"Received response: {response_data}")
        if(response_data['id'] != self.request_id_counter):
            return None
        try:
            # Read Content-Length header
            header_line = response_data
                        
            # Read the JSON content
            raw_response = response_data
            # print(f"Server -> Client: Content-Length: {content_length}\r\n\r\n{raw_response}") # For debugging

            return raw_response
        except Exception as e:
            # Try to get more info from server's stderr if an error occurs
            stderr_output = ""
            if self.process and self.process.stderr:
                # Non-blocking read for stderr
                self.process.stderr.flush()
                time.sleep(0.1) # give a moment for stderr to populate
                stderr_output = os.read(self.process.stderr.fileno(), 4096).decode('utf-8', errors='ignore')

            print(f"Error receiving or parsing response: {e}. Server stderr snippet:\n{stderr_output}")
            raise

    def initialize(self):
        print("\n[Client] Sending initialize...")
        initializeData = {
            "clientInfo": 
                {
                    "name": "TestClient", 
                    "version": "0.1"
                }
            }
        
        clientProtocolVersion = "2024-11-05"
        paramsData = {
            "protocolVersion": clientProtocolVersion,
            "capabilities": {
                "roots": {
                    "listChanged": True,
                },
                "sampling": {}
            },
        }
        self._send_request("initialize", clientinfo=initializeData, params=paramsData)
        response = self._receive_response()
        if("error" in response): # if no error then it means the initialization was successful
            error = response["error"]
            return error
        
        serverProtocolVersion = response["result"]["protocolVersion"]

        if(serverProtocolVersion != clientProtocolVersion):
            print("Client does not support server protocol version")
            return None
        
        capabilities = response["result"]["capabilities"]
        if("tools" in capabilities):
            self.hasTools = True

        if("resources" in capabilities):
            self.hasResources = True

        if("prompts" in capabilities):
            self.hasPrompts = True

        self.initialized = True
        method = "notifications/initialized"
        self._send_notification(method)
        print(f"[Client] Received mcp/initialize response: {json.dumps(response, indent=2)}")
        return response
    
    def _list_tools(self):
        print("\n[Client] Sending listTools...")
        method = "tools/list"
        self._send_request(method)
        response = self._receive_response()
        return response["result"]["tools"]
    
    def _call_tool(self,name,arguments=None):
        #args = json.dumps(arguments)
        params = {
            "name": name,
            "arguments": arguments
        }
        method = "tools/call"
        self._send_request(method,params)
        response = self._receive_response()
        return response
        

    
# Consumer to listen for response
def listen_for_response(client):
    global response_data
    consumer.subscribe([FROM_TOPIC])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error: ", msg.error())
            continue

        #print("Received message: " + msg.value().decode())
        response = json.loads(msg.value().decode())

        #print(f"Response: {response}")
        if response['id'] == client.request_id_counter:
            response_data = response['result']
            response_received.set()
            break

class Chatbot:

    def __init__(self):
        self.clients = []
        self.client_tools = {}
        self.availableTools = []
        if (ANTHROPIC_API_KEY == None):
            print("API key not set")
            raise ValueError("API Key must be set")
        self.anthropic = Anthropic(api_key=ANTHROPIC_API_KEY)


    def connect_to_servers(self, serverConfig):
        """Connect to all configured MCP servers."""
        try:
            with open(serverConfig, "r") as file:
                data = json.load(file)
            
            servers = data.get("mcpServers", {})
            
            for server_name, server_config in servers.items():
                self.connect_to_server(server_name, server_config)
                time.sleep(1)
        except Exception as e:
            print(f"Error loading server configuration: {e}")
            raise

    def connect_to_server(self, server_name: str, server_config: dict):
        try:
            client = MCPClient(server_config)
            client.initialize()

            if(client.initialized == False):
                print("Client server connection not initialized")
                return

            self.clients.append(client)

            if(client.hasTools):
                client.tools = client._list_tools()

            for tool in client.tools:
                self.client_tools[tool["name"]] = client
                self.availableTools.append({
                    "name": tool["name"],
                    "description": tool["description"],
                    "input_schema": tool["inputSchema"]
                })
        except Exception as e:
            print(f"Failed to connect to {server_name}: {e}")

    def process_query(self, query):
        messages = [{'role':'user', 'content':query}]
        response = self.anthropic.messages.create(max_tokens = 2024,
                                      model = 'claude-3-7-sonnet-20250219', 
                                      tools = self.availableTools,
                                      messages = messages)
        process_query = True
        while process_query:
            assistant_content = []
            for content in response.content:
                if content.type =='text':
                    print(content.text)
                    assistant_content.append(content)
                    if(len(response.content) == 1):
                        process_query= False
                elif content.type == 'tool_use':
                    assistant_content.append(content)
                    messages.append({'role':'assistant', 'content':assistant_content})
                    tool_id = content.id
                    tool_args = content.input
                    tool_name = content.name
                    
    
                    print(f"Calling tool {tool_name} with args {tool_args}")
                    
                    # Call a tool
                    client = self.client_tools[tool_name] # new
                    tool_response = client._call_tool(tool_name, arguments=tool_args)
                    messages.append({"role": "user", 
                                      "content": [
                                          {
                                              "type": "tool_result",
                                              "tool_use_id":tool_id,
                                              "content": tool_response["result"]["content"][0]["text"]
                                          }
                                      ]
                                    })
                    response = self.anthropic.messages.create(max_tokens = 2024,
                                      model = 'claude-3-7-sonnet-20250219', 
                                      tools = self.availableTools,
                                      messages = messages) 
                    
                    if(len(response.content) == 1 and response.content[0].type == "text"):
                        print(response.content[0].text)
                        process_query= False

    def chat_loop(self):
        """Run an interactive chat loop"""
        print("\nMCP Chatbot Started!")
        print("Type your queries or 'quit' to exit.")
        
        while True:
            try:
                query = input("\nQuery: ").strip()
        
                if query.lower() == 'quit':
                    break
                    
                self.process_query(query)
                print("\n")
                    
            except Exception as e:
                print(f"\nError: {str(e)}")

    def cleanup(self):
        print("Chatbot ended")



def main():
    chatbot = Chatbot()
    try:
        # the mcp clients and sessions are not initialized using "with"
        # like in the previous lesson
        # so the cleanup should be manually handled
        chatbot.connect_to_servers("server_config.json") # new! 
        chatbot.chat_loop()
    except Exception as e:
        print(f"Error starting host: {e}")
        
    finally:
        chatbot.cleanup() #new! 
    return

if __name__ == "__main__":
    main()