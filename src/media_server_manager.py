import os, sys
import paramiko
import argparse
import configparser

from tkinter import *

def run():
    
    # Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', dest='conf_file', help='REQUIRED: Configuration file', required=True)
    parser.add_argument('--nogui', action='store_true', help='Run in CLI-only mode. For debugging.')
    parser.add_argument('--pw_auth', action='store_true', help='Use password-based auth for SSH connection.')
    parser.add_argument('--cert_auth', action='store_true', help='User cert-based auth for SSH connection.')
    parser.add_argument('--local', action='store_true', help='Run manager for local machine, no SSH.')
    args = parser.parse_args()

    # Configurations
    configs = configparser.ConfigParser()
    configs.read(args.conf_file)
    if 'ssh' not in configs.sections():
        print('Missing required config file section: SSH')
        sys.exit(1)

    # Initialize SSH connection
    if not args.local:
        client = paramiko.client.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if args.pw_auth:
            client.connect(configs['ssh']['host'], username=configs['ssh']['username'], password=configs['ssh']['password'])
        elif args.cert_auth:
            pkey = paramiko.RSAKey.from_private_key_file(configs['ssh']['cert_file'])
            client.connect(configs['ssh']['host'], username=configs['ssh']['username'], pkey=pkey)
        else:
            print('Error: not running locally, but missing SSH auth specifier.')
            sys.exit(1)

    # Test SSH connection
    print('Testing SSH connection...')
    stdin, stdout, stderr = client.exec_command('lastlog')
    lines = stdout.read().decode()
    print(lines)
    client.close()

    return

if __name__ == "__main__":
    run()
