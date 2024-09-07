import argparse
import copy
import json
import os
import socket
import subprocess
import sys
import threading

from brpy_lib import (receive_bytes,
    OkayResponse,
    FailResponse,

    RenderRequestResponse,
    RenderFrameResponse,

    LocalRenderRequest,

    ServeRequest,
    Child
)


def register_at_parent(parent):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as parent_connection:
        parent_connection.connect(parent)
        request_header = json.dumps(ServeRequest(args.port).__dict__).encode()
        parent_connection.sendall(len(request_header).to_bytes(8))
        parent_connection.sendall(request_header)


def forward_requests(child, thread_id, request_header_size_raw, request_header_raw, blend_file=None):
    child_connection = get_child_connection(child, thread_id)

    child_connection.sendall(request_header_size_raw)
    child_connection.sendall(request_header_raw)

    if blend_file != None:
        child_connection.sendall(blend_file)

    child_response_header_size = int.from_bytes(receive_bytes(child_connection, 8))
    child_response_header = json.loads(receive_bytes(child_connection, child_response_header_size))


def forward_child_responses(client_connection, child_connection, send_lock, busy_lock, client_prefix):
    child_prefix = child_connection.getpeername()
    child_prefix = f"[{child_prefix[0]}:{child_prefix[1]}]"


    while True:
        response_header_size_raw = receive_bytes(child_connection, 8)
        response_header_size = int.from_bytes(response_header_size_raw)

        response_header_raw = receive_bytes(child_connection, response_header_size)
        response_header = json.loads(response_header_raw)


        response = b''

        if response_header['type'] == 'FRAME':
            response = receive_bytes(child_connection, response_header['frame_size'])
            print(f"{client_prefix} Forwarding frame {response_header['frame_number']} from {child_prefix}.")

        with send_lock:
            client_connection.sendall(response_header_size_raw)
            client_connection.sendall(response_header_raw)
            client_connection.sendall(response)


        if response_header['type'] == 'REQUEST':
            with busy_lock:
                busy_lock.notify()
            print(f"{client_prefix} Forwarding frame request from {child_prefix}.")


def handle_child_render(child_connection, render_requests, render_requests_lock, busy_lock, client_prefix):
    child_prefix = child_connection.getpeername()
    child_prefix = f"[{child_prefix[0]}:{child_prefix[1]}]"

    while True:
        while True:
            try:
                request = render_requests.pop(0)
                break
            except IndexError:
                with render_requests_lock:
                    render_requests_lock.wait()

        print(f"{client_prefix} Forwarding render request for frame {request['frames']} of session '{request['session']}' to {child_prefix}.")


        request = json.dumps(request).encode()
        child_connection.sendall(len(request).to_bytes(8))
        child_connection.sendall(request)


        with busy_lock:
            busy_lock.wait()


def send_frame(connection, send_lock, image, frame, client_prefix, session):
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
            image.split('.')[-1]
        ).__dict__
    ).encode()
    response = image_data


    with send_lock:
        connection.sendall(len(response_header).to_bytes(8))
        connection.sendall(response_header)
        connection.sendall(response)

    print(f"{client_prefix} Sent frame {frame} of session '{session}'.")


def handle_local_render(connection, render_requests, render_requests_lock, session, send_lock, client_prefix):
    blender_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    blender_port = args.port + 1


    while True:
        try:
            blender_socket.bind(('', blender_port))
            break
        except OSError:
            blender_port = (blender_port + 1) % 65536

    subprocess.Popen((blender, '-b', '-P', f"{os.path.dirname(__file__)}/brpy_render.py", '--', str(blender_port), session), stdout=subprocess.DEVNULL)

    blender_socket.listen()
    blender_socket, address = blender_socket.accept()

    while True:
        while True:
            try:
                request = render_requests.pop(0)
                break
            except IndexError:
                with render_requests_lock:
                    render_requests_lock.wait()


        frame = request['frames']
        session = request['session']


        # Send render request to locally running render script using bpy.
        request_header = json.dumps(LocalRenderRequest(session, frame).__dict__).encode()
        blender_socket.sendall(len(request_header).to_bytes(8))
        blender_socket.sendall(request_header)


        response_header_size = int.from_bytes(receive_bytes(blender_socket, 8))
        response_header = json.loads(receive_bytes(blender_socket, response_header_size))

        image = response_header['image_name']


        print(f"{client_prefix} Rendered frame {frame} of session '{session}', requesting more work.")

        response_header = json.dumps(RenderRequestResponse(1).__dict__).encode()
        with send_lock:
            connection.sendall(len(response_header).to_bytes(8))
            connection.sendall(response_header)


        threading.Thread(target=send_frame, args=(connection, send_lock, image, frame, client_prefix, session)).start()


def get_child_connection(child, thread_id):
    try:
        child_connection = child.connections[thread_id]
    except KeyError:
        child_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        child_connection.connect(child.address)
        child.connections[thread_id] = child_connection

    return child_connection


def handle_requests(connection):

    # Used to distinguish requests from different clients.
    client_name = connection.getpeername()
    client_prefix = f"[{client_name[0]}:{client_name[1]}]"


    # As multiple threads may try to send data to a client simultaneously, that needs to be guarded by a lock.
    send_lock = threading.Lock()


    startup = True

    thread_id = threading.get_ident()


    render_requests = []
    render_requests_lock = threading.Condition()


    for child in children:
        try:
            del child.connections[thread_id]
        except KeyError:
            pass


    with connection:
        while True:
            request_header_size_raw = receive_bytes(connection, 8)
            request_header_size = int.from_bytes(request_header_size_raw)

            request_header_raw = receive_bytes(connection, request_header_size)
            request_header = json.loads(request_header_raw)


            try:
                session = request_header['session']
            except KeyError:
                match request_header['type']:
                    case 'SERVE':
                        children.append(Child((client_name[0], request_header['port'])))
                        print(f"{client_prefix} New child node registered on port {request_header['port']}.")

                        continue


            if not session.isalnum():
                print(f"{client_prefix} Invalid session name of '{session}', breaking connection to client.")
                sys.exit()


            match request_header['type']:
                case 'UPLOAD':
                    print(f"{client_prefix} Receiving new file for session '{session}'.")
                    blend_file = receive_bytes(connection, request_header['size'])

                    with open(f"{session}.blend", 'wb') as file:
                        file.write(blend_file)
                    print(f"{client_prefix} Saved file '{session}.blend'.")


                    for child in children:
                        threading.Thread(
                            target=forward_requests,
                            args=(
                                child,
                                thread_id,
                                request_header_size_raw,
                                request_header_raw,
                                blend_file
                            )
                        ).start()


                    response_header = json.dumps(OkayResponse().__dict__).encode()

                case 'RENDER':
                    frames = request_header['frames']
                    if type(frames) == int:
                        render_requests.append(request_header)

                        with render_requests_lock:
                            render_requests_lock.notify()

                        print(f"{client_prefix} Received render request for frame {frames} of session '{session}'.")

                    else:
                        for frame in frames:
                            request = copy.copy(request_header)
                            request['frames'] = frame
                            render_requests.append(request)

                            print(f"{client_prefix} Received render request for frame {frame} of session '{session}'.")


                        with render_requests_lock:
                            render_requests_lock.notify(len(frames))


                    if startup:
                        threading.Thread(
                            target=handle_local_render,
                            args=(
                                connection,
                                render_requests,
                                render_requests_lock,
                                request_header['session'],
                                send_lock,
                                client_prefix
                            )
                        ).start()


                        if len(children) > 0:
                            response_header = json.dumps(RenderRequestResponse(len(children)).__dict__).encode()

                            with send_lock:
                                connection.sendall(len(response_header).to_bytes(8))
                                connection.sendall(response_header)


                            for child in children:
                                child_connection = get_child_connection(child, thread_id)
                                busy_lock = threading.Condition()

                                threading.Thread(
                                    target=handle_child_render,
                                    args=(
                                        child_connection,
                                        render_requests,
                                        render_requests_lock,
                                        busy_lock,
                                        client_prefix
                                    )
                                ).start()

                                threading.Thread(
                                    target=forward_child_responses,
                                    args=(
                                        connection,
                                        child_connection,
                                        send_lock,
                                        busy_lock,
                                        client_prefix
                                    )
                                ).start()


                        startup = False


                    continue

                case 'DELETE':
                    try:
                        os.remove(f"{session}.blend")
                        response_header = json.dumps(OkayResponse().__dict__).encode()
                        print(f"{client_prefix} File '{session}.blend' deleted.")
                    except FileNotFoundError:
                        response_header = json.dumps(FailResponse("File does not exist on server.").__dict__).encode()
                        print(f"{client_prefix} Could not remove nonexistant file '{session}.blend'.")

                    for child in children:
                        threading.Thread(
                            target=forward_requests,
                            args=(
                                child,
                                thread_id,
                                request_header_size_raw,
                                request_header_raw
                            )
                        ).start()


            connection.sendall(len(response_header).to_bytes(8))
            connection.sendall(response_header)


# Start of server program.

# Start of argument parsing.
parser = argparse.ArgumentParser(
    description="Run a server implementing the Blender Render Protocol (BRP).",
    formatter_class=argparse.RawTextHelpFormatter
)


parser.add_argument(
    'work_dir',
    metavar='working-directory',
    help="the working directory where .blend files and rendered images are stored\n\n"
)

parser.add_argument(
    'blender',
    help="""path to the Blender executable used for rendering

ensure that the appropriate rendering device(s) for Cycles is/are selected in the settings\n\n"""
)


parser.add_argument(
    '-p', '--port',
    metavar='port',
    type=int,
    default='21816',
    help="the port to listen on for incomming connections\n\n"
)

parser.add_argument(
    '--parents',
    metavar='parents',
    help="""a list of BRP servers at which to register as a child node

takes a list of comma separated pairs of address and port:

    "address port, address port, ..."\n\n"""
)

parser.add_argument(
    '--children',
    metavar='children',
    help="""a list of BRP servers that work should be offloaded to

takes the same list format as '--parents'\n\n"""
)


args = parser.parse_args()


if not os.path.exists(args.blender):
    sys.exit(f"'{args.blender}' does not exist, exiting.")
elif not os.path.isfile(args.blender):
    sys.exit(f"'{args.blender}' is not a file, exiting.")
elif not os.access(args.blender, os.X_OK):
    sys.exit(f"No permission to execute '{args.blender}', exiting.")
else:
    blender = os.path.abspath(args.blender)


if args.port < 0 or args.port > 65535:
    sys.exit(f"Port {args.port} is not within the range of 0 to 65535, exiting.")


# Change working directory last, so previous arguments are read from where the command was executed.
try:
    os.chdir(args.work_dir)
except FileNotFoundError:
    os.makedirs(args.work_dir)
    print(f"Created working directory '{args.work_dir}'.")
    os.chdir(args.work_dir)
except NotADirectoryError:
    sys.exit(f"'{args.work_dir}' is not a directory, exiting.")


parents = []
children = []


if args.parents != None:
    args.parents = args.parents.split(',')

    for parent in args.parents:
        parent = parent.split()
        parents.append((parent[0], int(parent[1])))


if args.children != None:
    args.children = args.children.split(',')

    for child in args.children:
        child = child.split()
        children.append(Child((child[0], int(child[1]))))


# Main thread starts to listen for connections.
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
    try:
        server.bind(('', args.port))
    except PermissionError:
        sys.exit(f"No permission to bind to port {args.port}, exiting.")
    server.listen()
    print(f"Listening on port {args.port} for incoming requests.")


    for parent in parents:
        threading.Thread(target=register_at_parent, args=(parent,)).start()


    # Keep listening for incoming connections and offload their requests to new threads.
    while True:
        connection, address = server.accept()
        threading.Thread(target=handle_requests, args=(connection,)).start()
        print(f"[{address[0]}:{address[1]}] New connection, handling requests.")
