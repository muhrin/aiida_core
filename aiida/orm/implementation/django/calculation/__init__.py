# -*- coding: utf-8 -*-
###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida_core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
import contextlib

from aiida.common.exceptions import LockError
from aiida.orm.implementation.django.node import Node
from aiida.orm.implementation.general.calculation import AbstractCalculation


class Calculation(AbstractCalculation, Node):
    _locked = False

    @contextlib.contextmanager
    def lock(self):
        """
        Context manager that, while active, will lock the node

        Trying to acquire this lock on an already locked node, will raise a LockError

        :raises LockError: the node is already locked in another context manager
        """
        from django.db import IntegrityError, OperationalError
        from aiida.backends.djsite.db.models import DbNode
        from aiida.common.exceptions import LockError

        # Have to employ different methods for stored and unstored nodes
        if not self.is_stored:
            if self._dbnode.public:
                raise LockError('Cannot lock calculation<{}> as it is already locked.'.format(self.pk))
            try:
                self._dbnode.public = True
                yield
            finally:
                self._dbnode.public = False
        else:
            # Have to go database on this m'a f'cker
            try:
                try:
                    DbNode.objects.update_or_create(pk=self.pk, public=False, defaults={'public': True})
                    # Set the local dbnode instance lock value (this is invalidated by the above call)
                    self._dbnode.public = True
                    yield
                finally:
                    self.force_unlock()

            except IntegrityError:
                # This means that the lock was activated and the above call tried to create a new row
                # with the same pk as this node.  This fails the uniqueness constraint and ends up here.
                raise LockError('cannot lock calculation<{}> as it is already locked.'.format(self.pk))

    @property
    def is_locked(self):
        """
        Returns whether the node is currently locked
        """
        return self.dbnode.public

    def force_unlock(self):
        """
        Force the unlocking of a node, by resetting the lock attribute

        This should only be used if one is absolutely clear that the node is no longer legitimately locked
        due to an active `lock` context manager, but rather the lock was not properly cleaned in exiting
        a previous lock context manager
        """
        self._dbnode.public = False
        self._dbnode.save(update_fields=('public',))
