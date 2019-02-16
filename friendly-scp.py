#!/usr/bin/env python3
import logging
import paramiko
import queue
import threading

def scp_copy(ipaddress, user='root', passwd='geocom', port=22):
    try:
        logging.debug('Opening connection to ' + ipaddress)
        transport = paramiko.Transport((ipaddress, port))
        transport.connect(username=user, password=passwd)
        sftp = paramiko.SFTPClient.from_transport(transport)
        try:
            logging.debug('Starting transfer to ' + ipaddress)
            sftp.put(args.file, args.dest)
        except Exception:
            logging.error('Error during file transfer to ' + ipaddress)
            raise
        logging.info('Succesful file transfer to host ' + ipaddress)
    except Exception:
        logging.error('Error while connecting to ' + ipaddress)
    finally:
        sftp.close()
        transport.close()

def start_workers(threads=15):
    global q
    q = queue.Queue()
    for i in range(threads):
        worker = threading.Thread(target=my_queue, args=(q,))
        worker.setDaemon(True)
        worker.start()

def my_queue(q):
    while True:
        ip = q.get()
        scp_copy(ip)
        q.task_done()

def add_to_queue():
    for line in open(args.list):
        q.put(line.rstrip())

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Massive sftp file transfer v1.0")
    parser.add_argument('-f', '--file', help='Local file you want to copy', required=True)
    parser.add_argument('-d', '--dest', help='Folder/file destination on remote hosts', required=True)
    parser.add_argument('-l', '--list', help='Local file containing hosts IP address', required=True)
    args = parser.parse_args()

    logging.getLogger("paramiko").setLevel(logging.ERROR)
    logging.basicConfig(filename='friendly-scp.log', level=logging.INFO)
    logging.info('Starting massive file transfer')

    start_workers()
    add_to_queue()
    q.join()
    logging.info('Massive file transfer finished.')
