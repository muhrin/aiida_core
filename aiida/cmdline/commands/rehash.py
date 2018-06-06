# -*- coding: utf-8 -*-
###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida_core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
import click

from aiida.cmdline.baseclass import VerdiCommand
from aiida.cmdline.commands import verdi_rehash
from aiida.cmdline.params import arguments
from aiida.cmdline.params.types.plugin import PluginParamType
from aiida.cmdline.utils import decorators, echo

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


class Rehash(VerdiCommand):
    """
    Re-hash nodes filtered by identifier and or node class
    """

    def run(self, *args):
        ctx = rehash.make_context('rehash', list(args))
        with ctx:
            rehash.invoke(ctx)


@verdi_rehash.command('rehash', context_settings=CONTEXT_SETTINGS)
@arguments.NODES()
@click.option('-e', '--entry-point', type=PluginParamType(group=('node', 'calculations', 'data'), load=True), default='node',
    help='restrict nodes which are re-hashed to instances that are a sub class of the class identified by this entry point')
@decorators.with_dbenv()
def rehash(nodes, entry_point):
    """
    Rehash all nodes in the database filtered by their identifier and/or based on their class
    """
    from aiida.orm.querybuilder import QueryBuilder

    if nodes:
        to_hash = [(node, ) for node in nodes if isinstance(node, entry_point)]
    else:
        qb = QueryBuilder()
        qb.append(entry_point, tag='node')
        to_hash = qb.all()

    if not to_hash:
        echo.echo_critical('no matching nodes found')

    for i, (node,) in enumerate(to_hash):
        if i % 100 == 0:
            echo.echo('.', nl=False)
        node.rehash()

    echo.echo_success('{} nodes re-hashed'.format(i + 1), prefix='\n')
