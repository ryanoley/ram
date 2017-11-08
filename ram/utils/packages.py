import os
import sys


def find_installed_ram():
    package = 'ram'
    system_paths = [p for p in sys.path if p != '']
    for p in system_paths:
        path = os.path.join(p, package)
        if os.path.isdir(path):
            return path
    return None
