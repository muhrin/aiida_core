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

from aiida.orm.implementation.django.node import Node
from aiida.orm.implementation.general.calculation import AbstractCalculation


class Calculation(AbstractCalculation, Node):

    @contextlib.contextmanager
    def lock(self):
        """
        Context manager that, while active, will lock the node

        Trying to acquire this lock on an already locked node, will raise a LockError

        :raises LockError: the node is already locked in another context manager
        """
        from django.db import transaction, IntegrityError
        from aiida.backends.djsite.db.models import DbNode
        from aiida.common.exceptions import LockError

        # No need to lock if it's an unstored node
        if not self.is_stored:
            if self._dbnode.public:
                raise LockError('cannot lock calculation<{}> as it is already locked.'.format(self.pk))
            else:
                self._dbnode.public = True
        else:
            with transaction.atomic():
                try:
                    # First try to select the calculation for update, which if it is already in a transaction
                    # in another process, will raise an IntegrityError
                    DbNode.objects.select_for_update(nowait=True).filter(pk=self.pk).first()

                    try:
                        # The first check won't catch cases where the node was already locked in the
                        # same interpreter, so for that case we try an update or create, which should
                        # fail if it was already locked in the same process
                        DbNode.objects.update_or_create(pk=self.pk, public=False, defaults={'public': True})
                        yield
                    finally:
                        self._dbnode.public = False
                        self._dbnode.save(update_fields=('public',))
                except IntegrityError:
                    raise LockError('cannot lock calculation<{}> as it is already locked.'.format(self.pk))

    def force_unlock(self):
        """
        Force the unlocking of a node, by resetting the lock attribute

        This should only be used if one is absolutely clear that the node is no longer legitimately locked
        due to an active `lock` context manager, but rather the lock was not properly cleaned in exiting
        a previous lock context manager
        """
        self._dbnode.public = False
