# -*- coding: utf-8 -*-
###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida_core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################

import uritools
import os.path
import plum
import os
import os.path as path
import portalocker
import portalocker.utils
import tempfile
import yaml

from aiida import orm

_RUNNING_DIRECTORY = path.join(tempfile.gettempdir(), "running")
_FINISHED_DIRECTORY = path.join(_RUNNING_DIRECTORY, "finished")
_FAILED_DIRECTORY = path.join(_RUNNING_DIRECTORY, "failed")

Bundle = plum.Bundle


# If portalocker accepts my pull request to have this incorporated into the
# library then this can be removed. https://github.com/WoLpH/portalocker/pull/34
class RLock(portalocker.Lock):
    """
    A reentrant lock, functions in a similar way to threading.RLock in that it
    can be acquired multiple times.  When the corresponding number of release()
    calls are made the lock will finally release the underlying file lock.
    """

    def __init__(
            self, filename, mode='a', timeout=portalocker.utils.DEFAULT_TIMEOUT,
            check_interval=portalocker.utils.DEFAULT_CHECK_INTERVAL, fail_when_locked=False,
            flags=portalocker.utils.LOCK_METHOD):
        super(RLock, self).__init__(filename, mode, timeout, check_interval,
                                    fail_when_locked, flags)
        self._acquire_count = 0

    def acquire(
            self, timeout=None, check_interval=None, fail_when_locked=None):
        if self._acquire_count >= 1:
            fh = self.fh
        else:
            fh = super(RLock, self).acquire(timeout, check_interval,
                                            fail_when_locked)
        self._acquire_count += 1
        return fh

    def release(self):
        if self._acquire_count == 0:
            raise portalocker.LockException(
                "Cannot release more times than acquired")

        if self._acquire_count == 1:
            super(RLock, self).release()
        self._acquire_count -= 1


Persistence = plum.PicklePersister

_GLOBAL_PERSISTENCE = None


def get_global_persistence():
    global _GLOBAL_PERSISTENCE

    if _GLOBAL_PERSISTENCE is None:
        _create_storage()

    return _GLOBAL_PERSISTENCE


def _create_storage():
    import aiida.common.setup as setup
    import aiida.settings as settings
    global _GLOBAL_PERSISTENCE

    parts = uritools.urisplit(settings.REPOSITORY_URI)
    if parts.scheme == u'file':
        WORKFLOWS_DIR = os.path.expanduser(
            os.path.join(parts.path, setup.WORKFLOWS_SUBDIR))

        _GLOBAL_PERSISTENCE = Persistence(
            pickle_directory=WORKFLOWS_DIR
        )


class AiiDAPersister(plum.Persister):
    """
    This node is responsible to taking saved process instance states and
    persisting them to the database.
    """
    CALC_NODE_CHECKPOINT_KEY = 'checkpoints'

    def save_checkpoint(self, process, tag=None):
        if tag is not None:
            raise NotImplementedError("Checkpoint tags not supported yet")

        bundle = Bundle(process)
        calc = process.calc
        calc._set_attr(self.CALC_NODE_CHECKPOINT_KEY,
                       yaml.dump(bundle))

    def load_checkpoint(self, pid, tag=None):
        if tag is not None:
            raise NotImplementedError("Checkpoint tags not supported yet")

        calc = orm.load_node(pid)
        return yaml.load(calc[self.CALC_NODE_CHECKPOINT_KEY])

    def get_checkpoints(self):
        """
        Return a list of all the current persisted process checkpoints
        with each element containing the process id and optional checkpoint tag

        :return: list of PersistedCheckpoint tuples
        """
        pass

    def get_process_checkpoints(self, pid):
        """
        Return a list of all the current persisted process checkpoints for the
        specified process with each element containing the process id and
        optional checkpoint tag

        :param pid: the process pid
        :return: list of PersistedCheckpoint tuples
        """
        pass

    def delete_checkpoint(self, pid, tag=None):
        orm.load_node(pid)._del_attr(self.CALC_NODE_CHECKPOINT_KEY)

    def delete_process_checkpoints(self, pid):
        """
        Delete all persisted checkpoints related to the given process id

        :param pid: the process id of the :class:`plum.process.Process`
        """
        pass
