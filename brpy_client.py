import argparse
import json
import os
import socket
import sys
import threading
import time

from brpy_lib import receive_bytes, SessionRequest, RenderRequest, UploadRequest


def request_frame(connection, frame, awaited_frames, server_prefix):
    print(f"{server_prefix} Sending request to render frame {frame}.")
    request_header = json.dumps(RenderRequest(args.session, frame, args.render_format).__dict__).encode()

    render_start = time.time()
    connection.sendall(len(request_header).to_bytes(8))
    connection.sendall(request_header)


    awaited_frames[frame] = render_start


def send_requests(server):
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
                if args.command == 'RENDER':
                    if len(frames) == 0:
                        print(f"{server_prefix} Could not connect, but all frames have already been handled, cancelling request.")
                        sys.exit()

                # Retry to connect for commands other than UPLOAD or if there are still frames left to be handled by RENDER command.
                print(f"{server_prefix} Could not connect, retrying in 10 seconds.")
                time.sleep(10)


        match args.command:
            case 'UPLOAD':

                # Send UPLOAD-request to upload .blend file.
                print(f"{server_prefix} Connected, uploading .blend file.")
                request_header = json.dumps(UploadRequest(args.session, blend_file_size).__dict__).encode()
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

            case 'RENDER':
                global global_frames_rendered
                global global_render_end

                frames_rendered = 0
                awaited_frames = {}


                # Initial render request, causing subsequent render request responses by the server.
                try:
                    frame = frames.pop(0)
                except IndexError:
                    sys.exit()

                request_frame(connection, frame, awaited_frames, server_prefix)


                # Start loop to render frames.
                while len(awaited_frames) > 0:

                    # Receive request to render more frames or a rendered frame.
                    response_header_size = int.from_bytes(receive_bytes(connection, 8, server_prefix))
                    response_header = json.loads(receive_bytes(connection, response_header_size, server_prefix))

                    match response_header['type']:
                        case 'REQUEST':
                            try:
                                frame = frames.pop(0)
                            except IndexError:
                                continue


                            awaited_frames[frame] = None    # Avoid race condition where frame arrives before next frame is requested,
                                                            # resulting in empty awaited_frames and early exit from loop.

                            threading.Thread(target=request_frame, args=(connection, frame, awaited_frames, server_prefix)).start()

                        case 'FRAME':
                            frame = response_header['frame_number']
                            image_data = receive_bytes(connection, response_header['frame_size'], server_prefix)
                            render_end = time.time()
                            print(f"{server_prefix} Received frame {frame} after {render_end - awaited_frames[frame]:.3f} seconds.")


                            try:
                                file_extension = response_header['file_extension']
                            except KeyError:
                                file_extension = ''

                            image = f"{frame:04d}"
                            if file_extension.isalnum():
                                image = f"{image}.{file_extension}"

                            with open(image, 'wb') as file:
                                file.write(image_data)
                            print(f"{server_prefix} Frame {frame} has been saved as '{image}'.")


                            # Increment the global counter of rendered frames and time the duration of rendering all frames if the last frame has just been rendered.
                            with global_frames_rendered_lock:
                                global_frames_rendered += 1
                            if global_frames_rendered == frames_count:
                                global_render_end = time.time()


                            frames_rendered += 1
                            del awaited_frames[frame]


                print(f"{server_prefix} Rendered {frames_rendered} frame(s) in total, {frames_rendered / frames_count:.2%} of all frames.")

            case 'DELETE':

                # Send DELETE-request to delete .blend file from server when all frames have been rendered.
                print(f"{server_prefix} Requesting deletion of .blend file.")
                request_header = json.dumps(SessionRequest('DELETE', args.session).__dict__).encode()
                connection.sendall(len(request_header).to_bytes(8))
                connection.sendall(request_header)


                response_header_size = int.from_bytes(receive_bytes(connection, 8, server_prefix))
                response_header = json.loads(receive_bytes(connection, response_header_size, server_prefix))

                match response_header['status']:
                    case 'OKAY':
                        print(f"{server_prefix} Successfully deleted .blend file from server.")
                    case 'FAIL':
                        print(f"{server_prefix} Failed to delete .blend file from server. Reason given: \"{response_header['error']}\"")


# Start of the program.
program_start = time.time()


# Start of argument parsing.
# First create parent parsers that can be reused for multiple subcommands.
server_parser = argparse.ArgumentParser(add_help=False)

server_parser.add_argument(
    'server_list',
    metavar='server-list',
    help="""a text file with each line representing a server which requests are sent to
lines in the server list should look like this:

    (#)server-address port

servers with a leading '#' are ignored

only as many servers are addressed as there are frames, starting from the top
listing the servers from fastest at the top to slowest at the bottom is advised

the same server may be listed multiple times
this can be useful for rendering slow encoding formats (PNG) in parallel\n\n"""
)


session_parser = argparse.ArgumentParser(add_help=False)

session_parser.add_argument(
    'session',
    help="""the name of the session by which a .blend file is identified
must be alphanumeric\n\n"""
)


# Create main parser.
parser = argparse.ArgumentParser(
    description="Distribute Blender render jobs via the Blender Render Protocol (BRP).",
    formatter_class=argparse.RawTextHelpFormatter
)

# Create subparsers for the different commands.
command_parsers = parser.add_subparsers(
    dest='command',
    required=True,
    description="The type of request that is sent to the server(s)."
)


# UPLOAD parser
parser_upload = command_parsers.add_parser(
    'UPLOAD',
    aliases=('upload',),
    parents=(server_parser, session_parser),
    description="Upload a .blend file of a session to the server(s).",
    help="upload a .blend file (identified by a session name) to the server(s)\n\n",
    formatter_class=argparse.RawTextHelpFormatter
)

parser_upload.add_argument(
    'blend_file',
    metavar='blend-file',
    help="""the .blend file to be stored on the server(s)
make sure all resources necessary for rendering are packed into the file

set an image format for render output unless rendering with the '-F' option\n\n"""
)


# RENDER parser
parser_render = command_parsers.add_parser(
    'RENDER',
    aliases=('render',),
    parents=(server_parser, session_parser),
    description="Send requests to render frames of a session within the given frame range.",
    help="request to render frame(s) of a session and save image(s)\n\n",
    formatter_class=argparse.RawTextHelpFormatter
)

parser_render.add_argument(
    'output_dir',
    metavar='output-directory',
    help="""the output directory rendered frames are saved to
created if not already present\n\n"""
)

parser_render.add_argument(
    'start_frame',
    metavar='start-frame',
    type=int,
    help="first frame of rendered frame range\n\n"
)

parser_render.add_argument(
    'end_frame',
    metavar='end-frame',
    type=int,
    nargs='?',
    help="""last frame of rendered frame range
if omitted, only start-frame is rendered\n\n"""
)

parser_render.add_argument(
    '-F',
    metavar='render-format',
    dest='render_format',
    help="""the format rendered frames are encoded in
takes the same values as the '-F' / '--render-format' option for Blender does

the use of PNGs is highly discouraged:
    it is likely to severely slow down rendering with high compression levels
    when using low compression levels, a main advantage of PNG is lost

consider using a fast encoding format like 'OPEN_EXR' to maximize throughput
OpenEXR export is incredibly fast while being suitable for post-production
optional lossy compression can also reduce file sizes massively\n\n"""
)


# DELETE parser
parser_delete = command_parsers.add_parser(
    'DELETE',
    aliases=('delete',),
    parents=(server_parser, session_parser),
    description="Delete the .blend file of a session.",
    help="delete the .blend file of a session\n\n",
    formatter_class=argparse.RawTextHelpFormatter
)


args = parser.parse_args()
args.command = args.command.upper()    # Due to subcommand aliases being saved as lowercase,
                                       # convert command to uppercase for further handling.


# Read servers from server list.
try:
    with open(args.server_list) as file:
        servers = file.read().splitlines()
except FileNotFoundError:
    sys.exit(f"The server list '{args.server_list}' does not exist, exiting.")
except IsADirectoryError:
    sys.exit(f"The server list '{args.server_list}' is a directory and not a file, exiting.")
except PermissionError:
    sys.exit(f"No permission to read from the server list '{args.server_list}', exiting")

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


# A server only accepts alphanumeric session names to avoid creating files in arbitrary paths like "../session.blend".
if not args.session.isalnum():
    sys.exit(f"The session name '{args.session}' is not alphanumeric, exiting.")


match args.command:
    case 'UPLOAD':
        try:
            with open(args.blend_file, 'rb') as file:
                blend_file = file.read()
        except FileNotFoundError:
            sys.exit(f"The .blend file '{args.blend_file}' does not exist, exiting.")
        except IsADirectoryError:
            sys.exit(f"'{args.blend_file}' is not a file but a directory, exiting.")
        except PermissionError:
            sys.exit(f"No permission to read from .blend file '{args.blend_file}', exiting.")

        blend_file_size = len(blend_file)

    case 'RENDER':
        if args.end_frame == None:
            frames = [args.start_frame]
        elif args.start_frame > args.end_frame:
            frames = list(range(args.end_frame, args.start_frame + 1))
        else:
            frames = list(range(args.start_frame, args.end_frame + 1))

        frames_count = len(frames)


        global_render_end = None                          # This value will later be set by the thread that receives the last frame.

        global_frames_rendered = 0                        # These two are necessary for measuring the total render-time correctly.
        global_frames_rendered_lock = threading.Lock()    # Because of timeouts due to reconnection attempts, the end of the render
                                                          # doesn't necessarily coincide with all threads joining up with the main thread.


        # Change to output directory last so all arguments are read from where the command was executed.
        try:
            os.chdir(args.output_dir)
        except FileNotFoundError:
            os.makedirs(args.output_dir)
            print(f"Created output directory '{args.output_dir}'.")
            os.chdir(args.output_dir)
        except NotADirectoryError:
            sys.exit(f"'{args.output_dir}' is not a directory, exiting.")


# Create threads that send requests to the servers.
threads = []
for server in servers:
    thread = threading.Thread(target=send_requests, args=(server,))
    thread.start()
    threads.append(thread)


# Wait for threads to finish.
for thread in threads:
    thread.join()


if args.command == 'RENDER':

    # Signal success and total render time to the user.
    global_render_time = global_render_end - program_start
    print(f"Done. {frames_count} frame(s) rendered in {global_render_time:.3f} seconds ({global_render_time / frames_count:.3f} seconds per frame on average).")
