#!/usr/bin/env python3
import logging
import paramiko
import queue
import socket
import threading

def add_to_queue():
    for line in open(args.list):
        q.put(line.rstrip())

def my_queue(q):
    while True:
        ip = q.get()
        scp_copy(ip)
        q.task_done()

def scp_copy(ipaddress, user='root', passwd='geocom', port=22, timeout=15):
    try:
        logging.debug('Opening connection to ' + ipaddress)
        sock = socket.socket(socket.AF_INET, 2049)
        sock.settimeout(timeout)
        sock.connect((ipaddress, port))
    except socket.timeout:
        logging.error('Error while connecting to ' + ipaddress)
        return
    try:
        logging.debug('Starting transfer to ' + ipaddress)
        transport = paramiko.Transport(sock)
        transport.connect(username=user, password=passwd)
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.put(args.file, args.dest)
    except Exception:
        logging.error('Error during file transfer to ' + ipaddress)
        return
    finally:
        sftp.close()
        transport.close()
    logging.info('Succesful file transfer to host ' + ipaddress)

def start_workers(threads=15):
    global q
    q = queue.Queue()
    for _ in range(threads):
        worker = threading.Thread(target=my_queue, args=(q,))
        worker.setDaemon(True)
        worker.start()

def run():
    logging.info('Starting massive file transfer')
    start_workers()
    add_to_queue()
    q.join()
    logging.info('Massive file transfer finished.')

logging.getLogger("paramiko").setLevel(logging.ERROR)
logging.basicConfig(filename='friendly-scp.log', level=logging.INFO)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Massive sftp file transfer v1.1")
    parser.add_argument('-f', '--file', help='Local file you want to copy', required=True)
    parser.add_argument('-d', '--dest', help='Remote file destination on remote hosts', required=True)
    parser.add_argument('-l', '--list', help='Local file containing remote hosts IP address', required=True)
    args = parser.parse_args()
    run()

