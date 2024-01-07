import sys
from socket import *

BYTES_SIZE = 1024

errorCodes = {
    '404': "404 NotFound",
    '405': "405 MethodNotAllowed",
    '200': "200 OK"
}

keys = dict()
counters = dict()

def sendResponse(responseMsg, socket):
    socket.send(responseMsg)

def parseMessage(message, socket):
    header, data = message.split(b'  ', 1)
    headers = getHeaders(header.decode().split(' '))
    responseMsg = requestHandler(headers, data)
    sendResponse(responseMsg, socket)

def getHandler(path, key):
    if (path == 'key'):
        if (key not in keys):
            # return 404 NotFound Error code
            response = errorCodes['404'] + '  '
            return response.encode()
        elif (key in keys and key in counters):
            # return 200 OK, Content-Length of value and the value itself with this key 
            response = (errorCodes['200'] + ' Content-Length ' + str(len(keys[key])) + '  ').encode() + keys[key]
            # and decrement counter in counters for this key by 1
            counters[key] -= 1
            # Remove key from counters and keys if counter reaches 0
            if (counters[key] <= 0):
                del counters[key]
                del keys[key]
            return response
        else: 
            # return 200 OK, Content-length of value and the value itself with this key
            response = (errorCodes['200'] + ' Content-Length ' + str(len(keys[key])) + '  ').encode() + keys[key]
            return response
    elif (path == 'counter'):
        if (key not in counters and key not in keys):
            # return 404 NotFound Error code
            response = errorCodes['404'] + '  '
            return response.encode()
        elif (key not in counters and key in keys):
            # return 200 OK, Content-Length value and Infinity
            response = errorCodes['200'] + ' Content-Length ' + str(len('Infinity')) + '  Infinity'
            return response.encode()
        else:
            # return 200 OK, Content-Length header and remaining retrieval times for this key
            response = errorCodes['200'] + ' Content-Length ' + str(len(str(counters[key]))) + '  ' + str(counters[key])
            return response.encode()

def postHandler(path, key, data):
    if (path == 'key'):
        if (key not in keys):
            # read the data and store with key in key store
            # return 200 OK Error code
            keys[key] = data
            response = errorCodes['200'] + '  '
            return response.encode()
        elif (key in counters and counters[key] > 0):
            # return 405 MethodNotAllowed
            response = errorCodes['405'] + '  '
            return response.encode()
        else: 
            # Update value for this key and return 200 OK Error code
            keys[key] = data
            response = errorCodes['200'] + '  '
            return response.encode()
    elif (path == 'counter'):
        if (key not in keys):
            # return 405 MethodNotAllowed code
            response = errorCodes['405'] + '  '
            return response.encode()
        elif (key not in counters):
            # return 200 OK and add this key to counters with new counter value
            counters[key] = int(data.decode())
            response = errorCodes['200'] + '  '
            return response.encode()
        else:
            # return 200 OK and update this key's counter value
            counters[key] += int(data.decode())
            response = errorCodes['200'] + '  '
            return response.encode()

def deleteHandler(path, key):
    if (path == 'key'):
        if (key not in keys):
            # return 404 NotFound error code
            response = errorCodes['404'] + '  '
            return response.encode()
        elif (key in keys and key in counters and counters[key] > 0):
            # return 405 MethodNotAllowed error code
            response = errorCodes['405'] + '  '
            return response.encode()
        else:
            # return 200 OK and value with this key
            # Delete the key-value pairs in both key and counter stores
            value = keys[key]

            del keys[key]
            if (key in counters and counters[key] <= 0):
                del counters[key]

    
            response = (errorCodes['200'] + ' Content-Length ' + str(len(value)) + '  ').encode() + value
            return response
    elif (path == 'counter'):
        if (key not in keys):
            # return 404 NotFound error code
            response = errorCodes['404'] + '  '
            return response.encode()
        else:
            # return 200 OK and content Length of value with this key
            # Delete the key-value pairs in both key and counter stores
            contentLength = len(str(counters[key]))
            response = errorCodes['200'] + ' Content-Length ' + str(contentLength) + '  ' + str(counters[key])
            del counters[key]

            return response.encode()

def getHeaders(headerSeg):
    requestTypes = ["POST", "GET", "DELETE"]
    headers = []

    for i in range(len(headerSeg)):
        if (headerSeg[i].upper() in requestTypes):
            reqType = headerSeg[i].upper()
            headers.append(reqType)
        
        if ("/key/" in headerSeg[i] or "/counter/" in headerSeg[i]):
            empty, path, key = headerSeg[i].split('/', 2)
            headers.append(path)
            headers.append(key)
        
        if (headerSeg[i].upper() == 'CONTENT-LENGTH'):
            contentLength = headerSeg[i + 1]
            if (contentLength.isdigit()):
                headers.append(contentLength)
    
    if (len(headers) < 4):
        headers.append("")

    return headers

def requestHandler(headerMsg, data):
    requestType, path, key, contentLength = headerMsg
    if (requestType == 'GET'):
        return getHandler(path, key)
    elif (requestType == 'POST'):
        return postHandler(path, key, data)
    elif (requestType == 'DELETE'):
        return deleteHandler(path, key)

def main():
    partial_request = b''
    content_length = None

    serverPort = int(sys.argv[1])
    serverSocket = socket(AF_INET, SOCK_STREAM)
    serverSocket.bind(('', serverPort))
    serverSocket.listen()
    
    # reading through chunks of data
    while True:
        connectionSocket, clientAddr = serverSocket.accept()
        while True:
            chunk = connectionSocket.recv(BYTES_SIZE)
            if not chunk:
                connectionSocket.close()
                break

            partial_request += chunk
            
            while True:
                if content_length is None:
                    if (b'  ' in partial_request):
                        request, partial_request = partial_request.split(b'  ', 1)
                        request += b'  '
                        headers = getHeaders(request.decode().split(' '))
                    else:
                        break
                    
                if (headers[-1] == ""):
                    parseMessage(request, connectionSocket)
                    content_length = None
                else:
                    content_length = int(headers[-1])
                    if (len(partial_request) >= content_length):
                        content = partial_request[:content_length]
                        partial_request = partial_request[content_length:]
                        request += content 

                        parseMessage(request, connectionSocket)

                        content_length = None
                    else: 
                        break

if __name__ == '__main__':
    main()









