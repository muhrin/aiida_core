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

from aiida.orm.implementation.general.calculation import AbstractCalculation
from aiida.orm.implementation.sqlalchemy.node import Node


class Calculation(AbstractCalculation, Node):

    @contextlib.contextmanager
    def lock(self):
        """
        Context manager that, while active, will lock the node

        Trying to acquire this lock on an already locked node, will raise a LockError

        :raises LockError: the node is already locked in another context manager
        """
        from aiida.backends.sqlalchemy.models.node import DbNode
        from aiida.backends.sqlalchemy import get_scoped_session
        from aiida.common.exceptions import LockError

        if not self.is_stored:
            # Do a local, 'in-memory' lock, means that behaviour of entering this context twice is same
            # as if it were stored
            if self._dbnode.public:
                raise LockError('cannot lock calculation<{}> as it is already locked.'.format(self.pk))
            else:
                self._dbnode.public = True
        else:
            session = get_scoped_session()
            res = session.query(DbNode). \
                filter_by(id=self.id, public=False). \
                update({'public': True})

            if res == 0:
                raise LockError('cannot lock calculation<{}> as it is already locked.'.format(self.pk))

            try:
                yield
            finally:
                self._dbnode.public = False
                self._dbnode.save()
                self._dbnode.session.commit()

    def force_unlock(self):
        """
        Force the unlocking of a node, by resetting the lock attribute

        This should only be used if one is absolutely clear that the node is no longer legitimately locked
        due to an active `lock` context manager, but rather the lock was not properly cleaned in exiting
        a previous lock context manager
        """
        self._dbnode.public = False
