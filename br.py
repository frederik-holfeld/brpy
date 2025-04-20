import bpy
import json
from pathlib import Path
import socket
import sys

args = sys.argv[sys.argv.index("--") + 1:]
connection = socket.create_connection(("localhost", args[0]))

old_blend = None
while True:
    size = bytearray(2)
    connection.recv_into(size)

    header = bytearray(int.from_bytes(size, 'little'))
    connection.recv_into(header)

    header = json.loads(header)
    match header["type"]:
        case "render":
            if header["blend"] != old_blend:
                bpy.ops.wm.open_mainfile(filepath=str(
                    Path(header["blend"]).absolute()
                ))

                old_blend = header["blend"]

            frame = header["frame"]
            bpy.context.scene.frame_current = frame

            path = str(Path(header["output"]) / f"{frame:04}")
            bpy.context.scene.render.filepath = path
            bpy.context.scene.render.use_file_extension = True

            bpy.ops.render.render(write_still=True)

            response = json.dumps({
                "type": "okay",
                "image": path + bpy.context.scene.render.file_extension
            }).encode()

            connection.sendall(len(response).to_bytes(2, 'little') + response)
        case "query":
            version = bpy.app.version

            cycles_preferences = bpy.context.preferences.addons['cycles'].preferences
            compute_device_type = cycles_preferences.compute_device_type

            active, inactive = [], []
            for device in cycles_preferences.get_devices_for_type(compute_device_type):
                if device['use'] == 1:
                    active.append(device['name'])
                else:
                    inactive.append(device['name'])

            response = json.dumps({
                'version': version,
                'compute_device_type': compute_device_type,
                'devices': {
                    'active': active,
                    'inactive': inactive
                }
            }).encode()

            connection.sendall(len(response).to_bytes(2, 'little') + response)
