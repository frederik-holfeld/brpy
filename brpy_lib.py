import sys


def receive_bytes(connection, size, prefix=''):
    buffer = bytearray(size)
    total_bytes_received = 0

    while True:
            bytes_received = connection.recv_into(memoryview(buffer)[total_bytes_received:])

            if bytes_received == 0:
                if prefix == '':
                    prefix = f"[{connection.getpeername()[0]}:{connection.getpeername()[1]}]"
                print(f"{prefix} Connection broken, exiting.")
                sys.exit()

            total_bytes_received += bytes_received

            if total_bytes_received == size:
                return buffer


class Request:
    def __init__(self, type):
        self.type = type

class SessionRequest(Request):
    def __init__(self, type, session):
        super().__init__(type)
        self.session = session

class UploadRequest(SessionRequest):
    def __init__(self, session, size):
        super().__init__('UPLOAD', session)
        self.size = size

class RenderRequest(SessionRequest):
    def __init__(self, session, frames, render_format):
        super().__init__('RENDER', session)
        self.frames = frames
        if render_format != None:
            self.render_format = render_format


class OkayResponse:
    def __init__(self):
        self.status = 'OKAY'

class FailResponse:
    def __init__(self, error):
        self.status = 'FAIL'
        self.error = error

class RenderOkayResponse(OkayResponse):
    def __init__(self, frame_size, file_extension):
        super().__init__()
        self.frame_size = frame_size
        self.file_extension = file_extension
