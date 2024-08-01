import json
import os
import socket
import subprocess
import sys
import threading

from brpy_lib import receive_bytes, OkayResponse, FailResponse, RenderRequestResponse, RenderFrameResponse


def render_frame(connection, session, request_header, send_lock):
    frame = request_header['frames']


    args = [blender, '-b', f"{work_dir}/{session}.blend", '-o', session]
    try:
        args += '-F', request_header['render_format']
    except KeyError:
        pass
    args += '-x', '1', '-f', str(frame), *options


    subprocess.run(args)


    with send_lock:
        response_header = json.dumps(RenderRequestResponse(1).__dict__).encode()
        connection.sendall(len(response_header).to_bytes(8))
        connection.sendall(response_header)


    # The working directory is assumed to be empty when starting the server.
    # Therefore the correct file should always be found,
    # unless when rendering the same session multiple times with different render formats concurrently,
    # which isn't guaranteed to work at the moment.
    with os.scandir() as files:
        for file in files:
            if file.name.startswith(f"{session}{frame:04d}.") and file.name != f"{session}.blend":
                image = file

    try:
        with open(image, 'rb') as file:
            image_data = file.read()
    except UnboundLocalError:
        print("Could not find saved frame, something must have gone wrong with the render. Exiting.")
        sys.exit()

    os.remove(image)


    response_header = json.dumps(
        RenderFrameResponse(
            len(image_data),
            frame,
            image.name.split('.')[-1]
        ).__dict__
    ).encode()
    response = image_data


    with send_lock:
        connection.sendall(len(response_header).to_bytes(8))
        connection.sendall(response_header)
        connection.sendall(response)


def handle_requests(connection):

    # Used to distinguish requests from different clients.
    client_name = connection.getpeername()
    client_prefix = f"[{client_name[0]}:{client_name[1]}]"


    # As multiple threads may try to send data to a client simultaneously, that needs to be guarded by a lock.
    send_lock = threading.Lock()


    with connection:
        while True:
            request_header_size = int.from_bytes(receive_bytes(connection, 8))
            request_header = json.loads(receive_bytes(connection, request_header_size))

            session = request_header['session']
            if not session.isalnum():
                print(f"{client_prefix} Invalid session name of '{session}', breaking connection to client.")
                sys.exit()


            response = b''

            match request_header['type']:
                case 'UPLOAD':
                    print(f"{client_prefix} Receiving new file for session '{session}'.")
                    blend_file = receive_bytes(connection, request_header['size'])

                    with open(f"{session}.blend", 'wb') as file:
                        file.write(blend_file)
                    print(f"{client_prefix} Saved file '{session}.blend'.")

                    response_header = json.dumps(OkayResponse().__dict__).encode()

                case 'RENDER':
                    threading.Thread(target=render_frame, args=(connection, session, request_header, send_lock)).start()
                    continue

                case 'DELETE':
                    try:
                        os.remove(f"{session}.blend")
                        response_header = json.dumps(OkayResponse().__dict__).encode()
                        print(f"{client_prefix} File '{session}.blend' deleted.")
                    except FileNotFoundError:
                        response_header = json.dumps(FailResponse("File does not exist on server.").__dict__).encode()
                        print(f"{client_prefix} Could not remove nonexistant file '{session}.blend'.")


            connection.sendall(len(response_header).to_bytes(8))
            connection.sendall(response_header)
            connection.sendall(response)


# Start of program.
if len(sys.argv) < 4:
    sys.exit("Valid arguments: working-directory Blender-executable port (Blender-options)\nToo few arguments, exiting.")


# Start of argument parsing.
work_dir = os.path.abspath(sys.argv[1])


if not os.path.exists(sys.argv[2]):
    sys.exit(f"'{sys.argv[2]}' does not exist, exiting.")
elif not os.path.isfile(sys.argv[2]):
    sys.exit(f"'{sys.argv[2]}' is not a file, exiting.")
elif not os.access(sys.argv[2], os.X_OK):
    sys.exit(f"No permission to execute '{sys.argv[2]}', exiting.")
else:
    blender = os.path.abspath(sys.argv[2])


try:
    port = int(sys.argv[3])
except ValueError:
    sys.exit(f"'{sys.argv[3]}' is not a number, exiting.")
if port < 0 or port > 65535:
    sys.exit(f"Port {port} is not within the range of 0 to 65535, exiting.")


# Appended at the end when running Blender. This argument is optional.
# Probably the most interesting usecase for this is setting the Cycles rendering device with "-- --cycles-device ...".
try:
    options = sys.argv[4].split()
except IndexError:
    options = ''


# Change working directory last, so previous arguments are read from where the command was executed.
try:
    os.chdir(work_dir)
except FileNotFoundError:
    os.makedirs(work_dir)
    print(f"Created working directory '{work_dir}'.")
    os.chdir(work_dir)
except NotADirectoryError:
    sys.exit(f"'{work_dir}' is not a directory, exiting.")


# Main thread starts to listen for connections.
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
    try:
        server.bind(('', port))
    except PermissionError:
        sys.exit(f"No permission to bind to port {port}, exiting.")
    server.listen()
    print(f"Listening on port {port} for incoming requests.")


    # Keep listening for incoming connections and offload their requests to new threads.
    while True:
        connection, address = server.accept()
        threading.Thread(target=handle_requests, args=(connection,)).start()
        print(f"[{address[0]}:{address[1]}] New connection, handling requests.")
