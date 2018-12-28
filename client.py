import socket
import time

START = 0
OK_ENTRY = 1
ERROR = 2

# Read buffer size
BUF_SIZE = 512

class Client:
    def __init__(self, host, port, timeout=None):
        try:
            if (timeout is None):
                self.sock = socket.create_connection((host, port))
            else:
                self.sock = socket.create_connection((host, port), timeout)
        except OSError:
            raise ClientError()
        # Need it later. b'' is empty byte array literal
        self.received_bytes = b''

    def _receive_string(self):
        # create a variable array of bytes
        collected_bytes = bytearray()
        while(True):
            
            if (len(self.received_bytes) == 0):
                self.received_bytes = self.sock.recv(BUF_SIZE)
            
            try:
                # looking for a newline in received_bytes
                idx = self.received_bytes.index(10)
                collected_bytes.extend(self.received_bytes[:idx + 1])
                if (idx == len(self.received_bytes) - 1):
                    self.received_bytes = b'' 
                else:
                    self.received_bytes = self.received_bytes[idx + 1:]
                return collected_bytes.decode("utf-8")
            except ValueError: # So the symbol is not found
                collected_bytes.extend(self.received_bytes)
                self.received_bytes = b''
    
    
    def _request_and_parse_reply(self, request_bytes, parse_entries):
        
        if (self.sock is None):
            raise ClientError("Closed socket", True)

        try:
            # Send a request to the server
            self.sock.sendall(request_bytes)
            # Empty dictionary for results
            entries = {}
            state = START
            error = ""

            while (True): 
                # We get a new line
                received_string = self._receive_string()
                if (state == START):
                    if (received_string == "ok\n"):
                        state = OK_ENTRY
                        continue
                    elif (received_string == "error\n"):
                        state = ERROR
                        continue
                    else:
                        raise ClientError("parse error", True)
                elif (state == OK_ENTRY):  
                    if (received_string == "\n"):
                        break
                    elif (parse_entries):
                        # Parsim metric
                        split_string = received_string[:-1].split()
                        if (len(split_string) != 3):
                            raise ClientError("entry parse error", True)
                        try:
                            
                            (name, value, timestamp) = [t(s) for t, s in zip((str, float, int), split_string)]
                        except TypeError:
                            
                            raise ClientError("entry parse error", True)
                        if (name in entries): #If we already had this metric add values to the list
                            entries[name].append((timestamp, value))
                        else:
                            
                            entries[name] = [(timestamp, value)]
                    else:
                        raise ClientError("ok parse error", True)
                elif (state == ERROR): 
                    if (received_string == "\n"): 
                        raise ClientError(error)
                    else: 
                        error += received_string
                else:
                    raise ClientError("parse error", True)
        except OSError:
            raise ClientError("OSError", True)
        if (parse_entries): # Sort responses before returning
            for key in entries:
                entries[key].sort(key=lambda entry: entry[0]) 
                
        return entries

    def get(self, key):
        get_bytes = "get {0}\n".format(key).encode("utf-8") 
        return self._request_and_parse_reply(get_bytes, True) # We send request and we return the dictionary, parse_entries = True


    def put(self, key, value, timestamp=None):
        timestamp = timestamp or int(time.time())
        put_bytes = "put {0} {1} {2}\n".format(key, value, timestamp) \
                                        .encode("utf-8")
        self._request_and_parse_reply(put_bytes, False) # We send a request and do not return anything, parse_entries = False

    def close(self):
        self.sock.close()
        self.sock = None

class ClientError(Exception):
    def __init__(self, message, client=False):
        self.message = message
        self.client = client

