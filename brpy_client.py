import json
import os
import socket
import sys
import threading
import time

from brpy_lib import receive_bytes, SessionRequest, RenderRequest, UploadRequest


def send_requests(server):
    global global_frames_rendered
    global global_render_end

    server_prefix = f"[{server[0]}:{server[1]}]"    # Indicates which server an output is associated with.


    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as connection:

        # Try connecting to server.
        while True:
            try:
                connection.connect(server)
                break
            except socket.gaierror:
                print(f"{server_prefix} Server is unknown, cancelling request.")
                sys.exit()
            except OSError:
                if len(frames) == 0:
                    print(f"{server_prefix} Could not connect, but all frames have already been handled, cancelling request.")
                    sys.exit()
                else:
                    print(f"{server_prefix} Could not connect, retrying in 10 seconds.")
                    time.sleep(10)


        # Send UPLOAD-request to upload .blend file.
        print(f"{server_prefix} Connected, uploading .blend file.")
        request_header = json.dumps(UploadRequest(session, blend_file_size).__dict__).encode()
        upload_start = time.time()
        connection.sendall(len(request_header).to_bytes(8))
        connection.sendall(request_header)
        connection.sendall(blend_file)


        # Receive status whether upload was successful or not.
        response_header_size = int.from_bytes(receive_bytes(connection, 8, server_prefix))
        response_header = json.loads(receive_bytes(connection, response_header_size, server_prefix))
        upload_end = time.time()

        match response_header['status']:
            case 'FAIL':
                print(f"{server_prefix} .blend file could not be uploaded, stopping request.")
                sys.exit()
            case 'OKAY':
                upload_time = upload_end - upload_start
                print(f"{server_prefix} File ({blend_file_size / 1000000:.1f} MB) uploaded successfully in {upload_time:.3f} seconds ({blend_file_size / upload_time / 1000000:.3f} MB/s).")


        # Start loop to render frames.
        frames_rendered = 0

        while True:
            try:
                frame = frames.pop(0)    # Operations on lists are apparently thread safe in Python, so no lock is needed here.
            except IndexError:

                # Send DELETE-request to delete .blend file from server when all frames have been rendered.
                print(f"{server_prefix} Requesting deletion of .blend file.")
                request_header = json.dumps(SessionRequest('DELETE', session).__dict__).encode()
                connection.sendall(len(request_header).to_bytes(8))
                connection.sendall(request_header)


                response_header_size = int.from_bytes(receive_bytes(connection, 8, server_prefix))
                response_header = json.loads(receive_bytes(connection, response_header_size, server_prefix))

                match response_header['status']:
                    case 'OKAY':
                        print(f"{server_prefix} Successfully deleted .blend file from server.")
                    case 'FAIL':
                        print(f"{server_prefix} Failed to delete .blend file from server. Reason given: \"{response_header['error']}\"")


                print(f"{server_prefix} Rendered {frames_rendered} frame(s) in total, {frames_rendered / frames_count:.2%} of all frames.")
                sys.exit()


            # Send RENDER-request.
            print(f"{server_prefix} Sending request to render frame {frame}.")
            request_header = json.dumps(RenderRequest(session, frame).__dict__).encode()
            render_start = time.time()
            connection.sendall(len(request_header).to_bytes(8))
            connection.sendall(request_header)


            # Receive rendered frame.
            response_header_size = int.from_bytes(receive_bytes(connection, 8, server_prefix))
            response_header = json.loads(receive_bytes(connection, response_header_size, server_prefix))
            image = receive_bytes(connection, response_header['frame_size'], server_prefix)
            render_end = time.time()
            print(f"{server_prefix} Received frame {frame} after {render_end - render_start:.3f} seconds.")

            with open(f"{frame:04d}.png", 'wb') as file:
                file.write(image)


            # Increment the global counter of rendered frames and time the duration of rendering all frames if the last frame has just been rendered.
            with global_frames_rendered_lock:
                global_frames_rendered += 1
            if global_frames_rendered == frames_count:
                global_render_end = time.time()

            frames_rendered += 1


# Start of the program.
program_start = time.time()
global_render_end = None    # This value will later be set by the thread that receives the last frame.


if len(sys.argv) < 6:
    sys.exit("Valid arguments: output-directory server-list .blend-file session-name start-frame (end-frame)\nToo few arguments, exiting.")


# Start of argument parsing.
output_dir = sys.argv[1]


# Read servers from server list.
try:
    with open(sys.argv[2]) as file:
        servers = file.read().splitlines()
except FileNotFoundError:
    sys.exit(f"The server list '{sys.argv[2]}' does not exist, exiting.")
except IsADirectoryError:
    sys.exit(f"The server list '{sys.argv[2]}' is a directory and not a file, exiting.")
except PermissionError:
    sys.exit(f"No permission to read from the server list '{sys.argv[2]}', exiting")

line = 0
while line < len(servers):
    try:
        if servers[line][0] == '#' or servers[line].isspace():    # Ignore servers that are commented out with a leading '#' and blank lines.
            servers.pop(line)
            continue
    except IndexError:
        servers.pop(line)
        continue
    server = servers[line].split()

    try:
        server[1] = int(server[1])
    except IndexError:
        sys.exit(f"Server entry {line + 1} '{servers[line]}' in the server list is malformed, must follow pattern '(#)server-address port'. Exiting.")
    except ValueError:
        sys.exit(f"Port '{server[1]}' of server {line + 1} is not a number, exiting.")
    if server[1] < 0 or server[1] > 65535:
        sys.exit(f"Port {server[1]} of server {line + 1} is not within the range of 0 to 65535, exiting.")
    servers[line] = tuple(server)
    line += 1

if len(servers) == 0:
    sys.exit("No active servers were found in the server list. Uncomment a server or add a new one to the server list. Exiting.")


try:
    with open(sys.argv[3], 'rb') as file:
        blend_file = file.read()
except FileNotFoundError:
    sys.exit(f"The .blend file '{sys.argv[3]}' does not exist, exiting.")
except IsADirectoryError:
    sys.exit(f"'{sys.argv[3]}' is not a file but a directory, exiting.")
except PermissionError:
    sys.exit(f"No permission to read from .blend file '{sys.argv[3]}', exiting.")

blend_file_size = len(blend_file)


# A server only accepts alphanumeric session names to avoid creating files in arbitrary paths like "../session.blend".
if not sys.argv[4].isalnum():
    sys.exit(f"The session name '{sys.argv[4]}' is not alphanumeric, exiting.")
else:
    session = sys.argv[4]


try:
    start_frame = int(sys.argv[5])
except ValueError:
    sys.exit(f"Start-frame '{sys.argv[5]}' is not a number, exiting.")

try:
    end_frame = int(sys.argv[6])
except ValueError:
    sys.exit(f"End-frame '{sys.argv[6]}' is not a number, exiting.")
except IndexError:
    end_frame = start_frame

if start_frame > end_frame:
    buffer = start_frame
    start_frame = end_frame
    end_frame = buffer

frames = list(range(start_frame, end_frame + 1))
frames_count = len(frames)

global_frames_rendered = 0                        # These two are necessary for measuring the total render-time correctly.
global_frames_rendered_lock = threading.Lock()    # Because of timeouts due to reconnection attempts, the end of the render
                                                  # doesn't necessarily coincide with all threads joining up with the main thread.


# Change to output directory last so all arguments are read from where the command was executed.
try:
    os.chdir(output_dir)
except FileNotFoundError:
    os.makedirs(output_dir)
    print(f"Created output directory '{output_dir}'.")
    os.chdir(output_dir)
except NotADirectoryError:
    sys.exit(f"'{output_dir}' is not a directory, exiting.")


# Create threads that send requests to the servers.
servers = servers[:frames_count]    # Truncate servers in case there are fewer frames than servers.
threads = []
for server in servers:
    thread = threading.Thread(target=send_requests, args=(server,))
    thread.start()
    threads.append(thread)


# Wait for threads to finish.
for thread in threads:
    thread.join()


# Signal success and total render time to the user.
global_render_time = global_render_end - program_start
print(f"Done. {frames_count} frame(s) rendered in {global_render_time:.3f} seconds ({global_render_time / frames_count:.3f} seconds per frame on average).")
