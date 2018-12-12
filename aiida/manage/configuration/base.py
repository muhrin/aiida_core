# -*- coding: utf-8 -*-
"""Base constants and defaults required for the configuration of an AiiDA instance."""
from __future__ import absolute_import

import os

from aiida.utils.find_folder import find_path

__all__ = ('DAEMON_DIR', 'DAEMON_LOG_DIR')

DEFAULT_AIIDA_CONFIG_FOLDER = '~/.aiida'
BASE_DAEMON_DIR = 'daemon'
BASE_DAEMON_LOG_DIR = 'log'

AIIDA_PATH = [os.path.expanduser(path) for path in os.environ.get('AIIDA_PATH', '').split(':') if path]
AIIDA_PATH.append(os.path.expanduser('~'))

for path in AIIDA_PATH:
    try:
        AIIDA_CONFIG_FOLDER = os.path.expanduser(str(find_path(root=path, dir_name='.aiida')))
        break
    except OSError:
        pass
else:
    AIIDA_CONFIG_FOLDER = os.path.expanduser(DEFAULT_AIIDA_CONFIG_FOLDER)

DAEMON_DIR = os.path.join(AIIDA_CONFIG_FOLDER, BASE_DAEMON_DIR)
DAEMON_LOG_DIR = os.path.join(DAEMON_DIR, BASE_DAEMON_LOG_DIR)
