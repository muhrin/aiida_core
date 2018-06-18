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
from aiida.common.exceptions import LockError


class Calculation(AbstractCalculation, Node):
    @contextlib.contextmanager
    def lock(self):
        from aiida.backends.sqlalchemy.models.node import DbNode
        from aiida.backends.sqlalchemy import get_scoped_session
        from sqlalchemy.exc import OperationalError

        # No need to lock if it's an unstored node
        already_locked = False
        if not self.is_stored:
            if self._dbnode.public:
                already_locked = True
            else:
                self._dbnode.public = True
        else:
            session = get_scoped_session()
            res = session.query(DbNode). \
                filter_by(id=self.id, public=False). \
                update({'public': True})

            if res == 0:
                already_locked = True

        if already_locked:
            raise LockError("Can't lock <{}>, already locked.".format(self.pk))

        try:
            yield
        finally:
            self._dbnode.public = False

    def force_unlock(self):
        self._dbnode.public = False
