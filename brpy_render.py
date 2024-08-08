import bpy

import json
import os
import socket
import sys

sys.path.append(os.path.dirname(__file__))
from brpy_lib import receive_bytes, LocalRenderResponse


def setup():

    # Ensure that the GPU is used for as many things as possible.
    # This should be faster and more efficient than using the CPU on the vast majority of systems.
    bpy.context.scene.render.compositor_device = 'GPU'
    bpy.context.scene.render.threads_mode = 'AUTO'    # Use all threads in case CPU is (also) used.

    bpy.context.scene.cycles.device = 'GPU'
    bpy.context.scene.cycles.denoising_use_gpu = True


# Start of render program.
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as connection:
    connection.connect(("localhost", int(sys.argv[5])))


    session = sys.argv[6]
    work_dir = os.getcwd()


    bpy.ops.wm.open_mainfile(filepath=f"{work_dir}/{session}.blend")

    setup()


    while True:
        request_header_size = int.from_bytes(receive_bytes(connection, 8))
        request_header = json.loads(receive_bytes(connection, request_header_size))


        if request_header['session'] != session:
            session = request_header['session']
            bpy.ops.wm.open_mainfile(filepath=f"{work_dir}/{session}.blend")

            setup()

        frame = request_header['frame']


        image_name = session + str(frame)
        bpy.context.scene.render.filepath = image_name
        bpy.context.scene.frame_current = frame


        bpy.ops.render.render(write_still=True)


        response_header = json.dumps(LocalRenderResponse(image_name + bpy.context.scene.render.file_extension).__dict__).encode()
        connection.sendall(len(response_header).to_bytes(8))
        connection.sendall(response_header)
