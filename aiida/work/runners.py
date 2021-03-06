import plum
import plum.rmq
from collections import namedtuple
from contextlib import contextmanager
import inspect
import logging

import aiida.orm
from . import persistence
from . import transport
from . import utils

__all__ = ['Runner', 'new_daemon_runner', 'new_runner',
           'set_runner']

_LOGGER = logging.getLogger(__name__)

ResultAndCalcNode = namedtuple("ResultWithPid", ["result", "calc"])
ResultAndPid = namedtuple("ResultWithPid", ["result", "pid"])

_runner = None


def get_runner():
    global _runner
    if _runner is None:
        _runner = new_runner()
    return _runner


def set_runner(runner):
    global _runner
    _runner = runner


def new_runner(**kwargs):
    """ Create a default runner optionally passing keyword arguments """
    return Runner(**kwargs)


def new_daemon_runner(rmq_prefix='aiida', rmq_create_connection=None):
    """ Create a daemon runner """
    runner = Runner({}, rmq_submit=False, enable_persistence=True)
    return runner


def convert_to_inputs(workfunction, *args, **kwargs):
    """
    """
    arg_labels, varargs, keywords, defaults = inspect.getargspec(workfunction)

    inputs = {}
    inputs.update(kwargs)
    inputs.update(dict(zip(arg_labels, args)))

    return inputs


def _object_factory(process_class, *args, **kwargs):
    return process_class(*args, **kwargs)


def _ensure_process(process, runner, input_args, input_kwargs, *args, **kwargs):
    """ Take a process class, a process instance or a workfunction along with
    arguments and return a process instance"""
    from aiida.work.processes import Process
    if isinstance(process, Process):
        assert len(input_args) == 0
        assert len(input_kwargs) == 0
        return process

    return _create_process(process, runner, input_args, input_kwargs, *args, **kwargs)


def _create_process(process, runner, input_args=(), input_kwargs={}, *args, **kwargs):
    """ Create a process instance from a process class or workfunction """
    inputs = _create_inputs_dictionary(process, *input_args, **input_kwargs)
    return _object_factory(process, runner=runner, inputs=inputs, *args, **kwargs)


def _create_inputs_dictionary(process, *args, **kwargs):
    """ Create an inputs dictionary for a process or workfunction """
    if utils.is_workfunction(process):
        inputs = convert_to_inputs(process, *args, **kwargs)
    else:
        inputs = kwargs
        assert len(args) == 0, "Processes do not take positional arguments"

    return inputs


class Runner(object):
    _persister = None
    _rmq_connector = None

    def __init__(self, rmq_config=None, loop=None, poll_interval=5.,
                 rmq_submit=False, enable_persistence=True, transp=None):
        self._loop = loop if loop is not None else plum.new_event_loop()
        self._poll_interval = poll_interval

        if transp is None:
            self._transport = transport.TransportQueue(self._loop)
        else:
            self._transport = transp

        if enable_persistence:
            self._persister = persistence.AiiDAPersister()

        self._rmq_submit = rmq_submit
        if rmq_config is not None:
            self._rmq_connector = plum.rmq.RmqConnector(**rmq_config)
            self._rmq = None  # construct from config
        elif self._rmq_submit:
            _LOGGER.warning('Disabling rmq submission, no RMQ config provided')
            self._rmq_submit = False

        # Save kwargs for creating child runners
        self._kwargs = {
            'rmq_config': rmq_config,
            'poll_interval': poll_interval,
            'rmq_submit': rmq_submit,
            'enable_persistence': enable_persistence
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def loop(self):
        return self._loop

    @property
    def rmq(self):
        return self._rmq

    @property
    def transport(self):
        return self._transport

    @property
    def persister(self):
        return self._persister

    def close(self):
        if self._rmq_connector is not None:
            self._rmq_connector.close()

    def run(self, process, *args, **inputs):
        """
        This method blocks and runs the given process with the supplied inputs.
        It returns the outputs as produced by the process. 
        
        :param process: The process class or workfunction to run
        :param inputs: Workfunction positional arguments
        :return: The process outputs
        """
        if utils.is_workfunction(process):
            return process(*args, **inputs)
        else:
            with self.child_runner() as runner:
                process = _ensure_process(process, runner, input_args=args, input_kwargs=inputs)
                return process.execute()

    def run_get_node(self, process, *args, **inputs):
        if utils.is_workfunction(process):
            return process.run_get_node(*args, **inputs)
        with self.child_runner() as runner:
            process = _ensure_process(process, runner, input_args=args, input_kwargs=inputs)
            return ResultAndCalcNode(process.execute(), process.calc)

    def run_get_pid(self, process, *args, **inputs):
        result, node = self.run_get_node(process, *args, **inputs)
        return ResultAndPid(result, node.pid)

    def submit(self, process_class, *args, **inputs):
        assert not utils.is_workfunction(process_class), "Cannot submit a workfunction"
        if self._rmq_submit:
            process = _create_process(process_class, self, input_args=args, input_kwargs=inputs)
            self.persister.save_checkpoint(process)
            # TODO: self.rmq.run(process.pid)
            return process.calc
        else:
            # Run in this runner
            process = _create_process(process_class, self, input_args=args, input_kwargs=inputs)
            process.play()
            return process.calc

    def call_on_legacy_workflow_finish(self, pk, callback):
        legacy_wf = aiida.orm.load_workflow(pk=pk)
        self._poll_legacy_wf(legacy_wf, callback)

    def call_on_calculation_finish(self, pk, callback):
        calc_node = aiida.orm.load_node(pk=pk)
        self._poll_calculation(calc_node, callback)

    def _submit(self, process, *args, **kwargs):
        pass

    @contextmanager
    def child_runner(self):
        runner = self._create_child_runner()
        try:
            yield runner
        finally:
            runner.close()

    def _create_child_runner(self):
        return Runner(transp=self._transport, **self._kwargs)

    def _poll_legacy_wf(self, workflow, callback):
        if workflow.has_finished_ok() or workflow.has_failed():
            self._loop.add_callback(callback, workflow.pk)
        else:
            self._loop.call_later(self._poll_interval, self._poll_legacy_wf, workflow, callback)

    def _poll_calculation(self, calc_node, callback):
        if calc_node.has_finished():
            self._loop.add_callback(callback, calc_node.pk)
        else:
            self._loop.call_later(self._poll_interval, self._poll_calculation, calc_node, callback)
