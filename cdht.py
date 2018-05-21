#!/usr/bin/python3
import re
import socket
import sys
import threading
import time

TIMER_LENGTH = 80
PEER_EXIT = False
FINAL_EXIT = False
PORT_OFFSET = 50000
this_peer = int(sys.argv[1])
next_peer = int(sys.argv[2])
after_peer = int(sys.argv[3])
p_peer1 = -1
p_peer2 = -1
p_peer1_confirm = False
p_peer2_confirm = False

next_peer_timer = TIMER_LENGTH
after_peer_timer = TIMER_LENGTH

address = "127.0.0.1"

next_port = PORT_OFFSET + next_peer
after_port = PORT_OFFSET + after_peer
this_port = PORT_OFFSET + this_peer

peer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
peer_socket.bind(('', this_port))
peer_socket.setblocking(0)

tcp_receive = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcp_receive.bind(('', this_port))
tcp_receive.setblocking(0)
tcp_receive.listen(5)


def ping_peers():
    while 1:
        if PEER_EXIT:
            break
        else:
            message = "Ping next " + str(this_peer)
            addr = (address, next_port)
            peer_socket.sendto(message.encode('utf-8'), addr)
            message = "Ping after " + str(this_peer)
            addr = (address, after_port)
            peer_socket.sendto(message.encode('utf-8'), addr)
            time.sleep(2)


def receive():
    global p_peer1_confirm
    global p_peer2_confirm
    global FINAL_EXIT
    while 1:
        try:
            if FINAL_EXIT:
                break
            else:
                if not PEER_EXIT:
                    message, addr = peer_socket.recvfrom(1024)
                    msg = message.decode('utf-8')
                    message_handler(msg)

                connection_socket, addr = tcp_receive.accept()
                sentence = connection_socket.recv(1024)
                sentence = sentence.decode('utf-8')
                if not PEER_EXIT:
                    message_handler(sentence)
                else:
                    sentence = sentence.strip()
                    confirm_match = re.match(r'Confirm ([0-9]+)', sentence)
                    if confirm_match:
                        if int(confirm_match.group(1)) == p_peer1:
                            p_peer1_confirm = True
                        if int(confirm_match.group(1)) == p_peer2:
                            p_peer2_confirm = True
                    if p_peer1_confirm and p_peer2_confirm:
                        FINAL_EXIT = True
        except socket.error:
            pass


def input_handler():
    global PEER_EXIT
    while 1:
        if PEER_EXIT:
            break
        else:
            for line in sys.stdin:
                file_match = re.match(r'request ([0-9]+)', line.strip())
                if line.strip() == "quit":
                    inform_peer_quit()
                elif file_match:
                    file_request(this_peer, file_match.group(1))


def inform_peer_quit():
    global PEER_EXIT
    message = str(this_peer) + " departing " + str(next_peer) + " " + str(after_peer)
    PEER_EXIT = True

    tcp_send = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_send.connect((address, p_peer1 + PORT_OFFSET))
    tcp_send.send(message.encode('utf-8'))
    tcp_send.close()

    tcp_send = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_send.connect((address, p_peer2 + PORT_OFFSET))
    tcp_send.send(message.encode('utf-8'))
    tcp_send.close()


# String Format:
# file request peer, requests , file name, sending message peer
def file_request(peer, file_name):
    tcp_send = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    message = str(peer) + " requests " + file_name + " " + str(this_peer)
    tcp_send.connect((address, next_port))
    tcp_send.send(message.encode('utf-8'))
    tcp_send.close()
    print("File request message for " + file_name + " has been sent to my successor.")


def file_request_forward(peer, file_name):
    tcp_send = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    message = str(peer) + " requests " + file_name + " " + str(this_peer)
    tcp_send.connect((address, next_port))
    tcp_send.send(message.encode('utf-8'))
    tcp_send.close()
    print("File request message has been forwarded to my successor.")


def message_handler(msg):
    msg = msg.strip()
    global next_peer_timer
    global after_peer_timer
    global next_peer
    global after_peer
    global after_port
    global p_peer1
    global p_peer2
    ping_match = re.match(r'Ping ([a-z]+) ([0-9]+)', msg)
    ping_response_match = re.match(r'Ping Response ([0-9]+)', msg)
    file_match = re.match(r'([0-9]+) requests ([0-9]+) ([0-9]+)', msg)
    file_response_match = re.match(r'([0-9]+) request response ([0-9]+) ([0-9]+)', msg)
    depart_match = re.match(r'([0-9]+) departing ([0-9]+) ([0-9]+)', msg)
    query_match = re.match(r'query ([0-9]+)', msg)
    query_response_match = re.match(r'query response ([0-9]+)', msg)

    if ping_match:
        print("A ping request message was received from Peer " + ping_match.group(2) + ".")
        if ping_match.group(1) == "next":
            p_peer1 = int(ping_match.group(2))
        elif ping_match.group(1) == "after":
            p_peer2 = int(ping_match.group(2))
        ping_response(int(ping_match.group(2)))
    elif ping_response_match:
        print("A ping response message was received from Peer " + ping_response_match.group(1) + ".")
        if int(ping_response_match.group(1)) == next_peer:
            next_peer_timer = TIMER_LENGTH
        if int(ping_response_match.group(1)) == after_peer:
            after_peer_timer = TIMER_LENGTH
    elif file_match:
        if peer_file_comparison(int(file_match.group(2)), int(file_match.group(3))):
            print("File " + file_match.group(2) + " is here.")
            file_response(file_match.group(1), file_match.group(2))
        else:
            print("File " + file_match.group(2) + " is not stored here.")
            file_request_forward(file_match.group(1), file_match.group(2))
    elif file_response_match:
        print("Received a response message from peer " + file_response_match.group(3) + ", which has the file "
              + file_response_match.group(2) + ".")
    elif depart_match:
        peer_update(depart_match.group(1), depart_match.group(2), depart_match.group(3))
    elif query_match:
        peer_query_response(int(query_match.group(1)))
    elif query_response_match:
        after_peer = int(query_response_match.group(1))
        after_port = PORT_OFFSET + after_peer
        print("My second successor is now peer " + str(after_peer))
        after_peer_timer = TIMER_LENGTH


def peer_update(leaving_peer, peer_one, peer_two):
    global next_peer
    global after_peer
    global next_port
    global after_port
    global next_peer_timer
    global after_peer_timer

    # send confirmation message
    message = "Confirm " + str(this_peer)
    tcp_send = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_send.connect((address, int(leaving_peer)+PORT_OFFSET))
    tcp_send.send(message.encode('utf-8'))
    tcp_send.close()

    print("Peer " + leaving_peer + " will depart from the network.")
    if next_peer == int(leaving_peer):
        next_peer = int(peer_one)
        after_peer = int(peer_two)
        next_peer_timer = TIMER_LENGTH
        after_peer_timer = TIMER_LENGTH
        next_port = PORT_OFFSET + next_peer
        after_port = PORT_OFFSET + after_peer
        print("My first successor is now peer " + str(next_peer))
        print("My second successor is now peer " + str(after_peer))
    elif after_peer == int(leaving_peer):
        after_peer = int(peer_one)
        after_port = PORT_OFFSET + after_peer
        after_peer_timer = TIMER_LENGTH
        print("My first successor is now peer " + str(next_peer))
        print("My second successor is now peer " + str(after_peer))


def peer_file_comparison(file, prev_peer):
    file_hash = hash_function(file)
    if file_hash == this_peer:
        return True
    elif prev_peer > this_peer > file_hash:  # when file_hash is smaller than the lowest peer
        return True
    elif file_hash > prev_peer > this_peer:  # when this peer is lowest in cdht
        return True
    elif prev_peer < file_hash < this_peer:  # general case
        return True
    else:
        return False


def file_response(peer, file_name):
    tcp_send = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    message = str(peer) + " request response " + str(file_name) + " " + str(this_peer)

    tcp_send.connect((address, PORT_OFFSET + int(peer)))
    tcp_send.send(message.encode('utf-8'))
    tcp_send.close()

    print("A response message, destined for peer " + peer + ", has been sent.")


def ping_response(peer):
    message = "Ping Response " + str(this_peer)
    addr = (address, peer + PORT_OFFSET)
    peer_socket.sendto(message.encode('utf-8'), addr)


def check_peer_alive():
    global next_peer
    global after_peer
    global next_peer_timer
    global after_peer_timer
    global next_port
    while 1:
        if PEER_EXIT:
            break
        else:
            time.sleep(0.1)
            next_peer_timer -= 1
            after_peer_timer -= 1
            if next_peer_timer == 0:
                print("Peer " + str(next_peer) + " is no longer alive.")
                next_peer = after_peer
                next_peer_timer = TIMER_LENGTH
                next_port = PORT_OFFSET + next_peer
                print("My first successor is now peer " + str(next_peer))
                peer_query(next_peer)
            if after_peer_timer == 0:
                print("Peer " + str(after_peer) + " is no longer alive.")
                print("My first successor is now peer " + str(next_peer))
                peer_query(next_peer)


def peer_query(query_peer):
    tcp_send = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    message = "query " + str(this_peer)
    tcp_send.connect((address, query_peer + PORT_OFFSET))
    tcp_send.send(message.encode('utf-8'))
    tcp_send.close()


def peer_query_response(query_peer):
    tcp_send = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    message = "query response " + str(next_peer)
    tcp_send.connect((address, query_peer + PORT_OFFSET))
    tcp_send.send(message.encode('utf-8'))
    tcp_send.close()


def hash_function(file_name):
    return file_name % 256


class PingThread(threading.Thread):
    def __init__(self):
        super(PingThread, self).__init__()

    def run(self):
        ping_peers()


class ReceiveThread(threading.Thread):
    def __init__(self):
        super(ReceiveThread, self).__init__()

    def run(self):
        receive()


class InputThread(threading.Thread):
    def __init__(self):
        super(InputThread, self).__init__()

    def run(self):
        input_handler()


class PeerAliveThread(threading.Thread):
    def __init__(self):
        super(PeerAliveThread, self).__init__()

    def run(self):
        check_peer_alive()


thread1 = PingThread()
thread2 = ReceiveThread()
thread3 = InputThread()
thread4 = PeerAliveThread()
thread1.daemon = True
thread1.start()
thread2.daemon = True
thread2.start()
thread3.daemon = True
thread3.start()
thread4.daemon = True
thread4.start()
while 1:
    time.sleep(0.2)
    if FINAL_EXIT:
        sys.exit(0)
