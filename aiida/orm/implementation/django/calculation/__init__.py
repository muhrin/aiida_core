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
        from django.db import transaction
        from aiida.backends.djsite.db.models import DbNode
        try:
            with transaction.atomic():
                DbNode.objects.select_for_update(nowait=True).filter(pk=self.pk).first()
                yield
        # TODO: Reraise lock error from AiiDa
        finally:
            pass
