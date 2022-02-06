
# do not import anything else from loss_socket besides LossyUDP
import time
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import struct
import hashlib
from concurrent.futures import ThreadPoolExecutor


class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.window = 0
        self.seq = 0
        self.buffer = {}
        self.ackbuffer = set()
        self.closed = False
        self.fin = False

        executor = ThreadPoolExecutor(max_workers = 1)
        executor.submit(self.listener)

    def send(self, data_bytes: bytes) -> None:
        counter = 0
        data = []
        length = len(data_bytes)

        # check if it is ack message
        if "ACK_THIS_MESSAGE" in data_bytes.decode():
            num = int(data_bytes.decode().split()[1])

            result = hashlib.md5((str(num) + '01').encode())

            # 0 for data and 1 for ack
            temp_struct = struct.pack("iib1447s16s", num, 0, 1, b"",result.digest())
            self.socket.sendto(temp_struct, (self.dst_ip, self.dst_port))
            return


        while length > counter+1447:
            data.append(data_bytes[counter:counter+1447])
            counter += 1447
        if length > counter:
            data.append(data_bytes[counter:])
        for element in data:
            length = len(element)

            result = hashlib.md5((str(self.seq) + str(length) + '0').encode())

            # 0 for data and 1 for ack
            temp_struct = struct.pack("iib1447s16s", self.seq, length, 0, element,result.digest())
            self.ackbuffer.add(self.seq)


            self.socket.sendto(temp_struct, (self.dst_ip, self.dst_port))

            count = 0
            while self.seq in self.ackbuffer:

                time.sleep(.01)
                count += .01
                if count >= .25:
                    count = 0
                    self.socket.sendto(temp_struct, (self.dst_ip, self.dst_port))
            self.seq += 1


    def recv(self) -> bytes:
        retdata = b""
        while len(self.buffer) != 0 and self.window in self.buffer:
            body = self.buffer[self.window][0]
            retdata += body[:self.buffer[self.window][1]]
            self.buffer.pop(self.window)
            self.window += 1

        return retdata

    def listener(self):
        while not self.closed:
            try:
                data, addr = self.socket.recvfrom()

                # Check if data is empty
                if len(data) == 0:
                    continue

                data_struct = struct.unpack("iib1447s16s", data)
                num = data_struct[0]
                length = data_struct[1]
                type = data_struct[2]
                body = data_struct[3]
                hashval = data_struct[4]
                if hashval != (hashlib.md5((str(num)+str(length)+str(type)).encode())).digest():
                    continue

                if type == 1 and (self.ackbuffer is not None) and (num in self.ackbuffer):
                    self.ackbuffer.remove(num)
                if type == 0:
                    if body[:length].decode() == f"FIN All finished":
                        self.fin = True
                    self.buffer[num] = (body,length)
                    self.send(f"ACK_THIS_MESSAGE {data_struct[0]}".encode())


            except Exception as e:
                print("LISTERNER DIED!")
                print(e)

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        while self.ackbuffer:
            time.sleep(.01)

        self.send(f"FIN All finished".encode())

        while self.fin == False:
            time.sleep(.01)

        time.sleep(2)

        self.closed = True
        self.socket.stoprecv()


# # do not import anything else from loss_socket besides LossyUDP
# import time
# from lossy_socket import LossyUDP
# # do not import anything else from socket except INADDR_ANY
# from socket import INADDR_ANY
# import struct
# import heapq
# from concurrent.futures import ThreadPoolExecutor
#
#
# class Streamer:
#     def __init__(self, dst_ip, dst_port,
#                  src_ip=INADDR_ANY, src_port=0):
#         """Default values listen on all network interfaces, chooses a random source port,
#            and does not introduce any simulated packet loss."""
#         self.socket = LossyUDP()
#         self.socket.bind((src_ip, src_port))
#         self.dst_ip = dst_ip
#         self.dst_port = dst_port
#         self.window = 0
#         self.seq = 0
#         self.buffer = {}
#         self.ackbuffer = set()
#         self.closed = False
#         self.fin = False
#
#         executor = ThreadPoolExecutor(max_workers = 1)
#         executor.submit(self.listener)
#
#     def send(self, data_bytes: bytes) -> None:
#         counter = 0
#         data = []
#         length = len(data_bytes)
#
#         # check if it is ack message
#         if "ACK_THIS_MESSAGE" in data_bytes.decode():
#             num = int(data_bytes.decode().split()[1])
#             # 0 for data and 1 for ack
#             temp_struct = struct.pack("iib1463s", num, 0, 1, b"")
#             self.socket.sendto(temp_struct, (self.dst_ip, self.dst_port))
#             return
#
#
#         while length > counter+1463:
#             data.append(data_bytes[counter:counter+1463])
#             counter += 1463
#         if length > counter:
#             data.append(data_bytes[counter:])
#         for element in data:
#             length = len(element)
#             # 0 for data and 1 for ack
#             temp_struct = struct.pack("iib1463s", self.seq, length, 0, element)
#             self.ackbuffer.add(self.seq)
#
#
#             self.socket.sendto(temp_struct, (self.dst_ip, self.dst_port))
#
#             count = 0
#             while self.seq in self.ackbuffer:
#
#                 time.sleep(.01)
#                 count += .01
#                 if count >= .25:
#                     count = 0
#                     self.socket.sendto(temp_struct, (self.dst_ip, self.dst_port))
#             self.seq += 1
#
#
#     def recv(self) -> bytes:
#         retdata = b""
#         while len(self.buffer) != 0 and self.window in self.buffer:
#             body = self.buffer[self.window][0]
#             retdata += body[:self.buffer[self.window][1]]
#             self.buffer.pop(self.window)
#             self.window += 1
#
#         return retdata
#
#     def listener(self):
#         while not self.closed:
#             try:
#                 data, addr = self.socket.recvfrom()
#
#                 # Check if data is empty
#                 if len(data) == 0:
#                     continue
#
#                 data_struct = struct.unpack("iib1463s", data)
#                 num = data_struct[0]
#                 length = data_struct[1]
#                 type = data_struct[2]
#                 body = data_struct[3]
#
#                 if type == 1 and (self.ackbuffer is not None) and (num in self.ackbuffer):
#                     self.ackbuffer.remove(num)
#                 if type == 0:
#                     if body[:length].decode() == f"FIN All finished":
#                         self.fin = True
#                     self.buffer[num] = (body,length)
#                     self.send(f"ACK_THIS_MESSAGE {data_struct[0]}".encode())
#
#
#             except Exception as e:
#                 print("LISTERNER DIED!")
#                 print(e)
#
#     def close(self) -> None:
#         """Cleans up. It should block (wait) until the Streamer is done with all
#            the necessary ACKs and retransmissions"""
#         while self.ackbuffer:
#             time.sleep(.01)
#
#         self.send(f"FIN All finished".encode())
#
#         while self.fin == False:
#             time.sleep(.01)
#
#         time.sleep(2)
#
#         self.closed = True
#         self.socket.stoprecv()
