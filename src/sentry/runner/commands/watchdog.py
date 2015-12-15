#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# ---------------------------------------------------------------------
# watchdog
# ---------------------------------------------------------------------
# Copyright (c) 2015 Merchise Autrement and Contributors
# All rights reserved.
#
# This is free software; you can redistribute it and/or modify it under the
# terms of the LICENCE attached (see LICENCE file) in the distribution
# package.
#
# Created on 2015-12-15

'''This is simply our monitoring script to:

- Watch if the worker is pooling like crazy.
- If so trigger a flushdb and SIGHUP the worker master process.

'''

from __future__ import (division as _py3_division,
                        print_function as _py3_print,
                        absolute_import as _py3_abs_import)


import logging
import click

from celery.events.snapshot import Polaroid
from sentry.runner.decorators import configuration


MINUTE = 60
TWO_MINUTES = 2 * MINUTE
_logger = logging.getLogger(__name__)


class WatchdogTicker(Polaroid):
    def __init__(self, *args, **kwargs):
        super(WatchdogTicker, self).__init__(*args, **kwargs)
        self.count = -1

    def on_shutter(self, state):
        _logger.debug('Ticking with %d events', state.event_count)
        if self.count == state.event_count:
            # No new events since the last time we checked
            self.restart_worker(state)
        else:
            self.count = state.event_count

    def restart_worker(self, state):
        import os
        import signal
        import psutil
        # TODO: Take redis DB from the BROKER_URL
        os.system('redis-cli -n 2 flushdb')
        for worker in state.workers.values():
            _logger.warn('Sending SIGKILL to worker with PID %d',
                         worker.pid)
            try:
                children = []
                for proc in psutil.process_iter():
                    pinfo = proc.as_dict(['ppid'])
                    pid = pinfo['ppid']
                    if pid == worker.pid:
                        children.append(pid)
                os.kill(worker.pid, signal.SIGKILL)
                for pid in children:
                    if psutil.pid_exists(pid):
                        try:
                            os.kill(pid, signal.SIGKILL)
                        except:
                            pass
            except:
                _logger.exception('Killing worker')


@click.command()
@click.option('--freq', default=TWO_MINUTES,
              help='Frequency of snapshots.')
@configuration
def watchdog(freq):
    '''Watch for centry worker liveness.'''
    from celery.events.snapshot import evcam
    _logger.setLevel(logging.DEBUG)
    _logger.info('Starting the watchdog process')
    # TODO fully qualified name
    return evcam(__name__ + '.' + WatchdogTicker.__name__, freq=freq)
