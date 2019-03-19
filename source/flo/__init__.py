#!/usr/bin/env python
# encoding: utf-8
"""

Purpose: Run the hirs_tpw_daily package

Copyright (c) 2015 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import os
from os.path import basename, dirname, curdir, abspath, isdir, isfile, exists, splitext, join as pjoin
import sys
from glob import glob
import shutil
import logging
import traceback
from subprocess import CalledProcessError
import numpy as np

from flo.computation import Computation
from flo.builder import WorkflowNotReady
from timeutil import TimeInterval, datetime, timedelta, round_datetime
from flo.util import augmented_env, symlink_inputs_to_working_dir
from flo.product import StoredProductCatalog

import sipsprod
from glutil import (
    check_call,
    dawg_catalog,
    delivered_software,
    #support_software,
    runscript,
    #prepare_env,
    #nc_gen,
    nc_compress,
    reraise_as,
    #set_official_product_metadata,
    FileNotFound
)
import flo.sw.hirs_tpw_orbital as hirs_tpw_orbital
from flo.sw.hirs2nc.delta import DeltaCatalog
from flo.sw.hirs2nc.utils import link_files

# every module should have a LOG object
LOG = logging.getLogger(__name__)

def set_input_sources(input_locations):
    global delta_catalog
    delta_catalog = DeltaCatalog(**input_locations)

class HIRS_TPW_DAILY(Computation):

    parameters = ['granule', 'satellite', 'hirs2nc_delivery_id', 'hirs_avhrr_delivery_id',
                  'hirs_csrb_daily_delivery_id', 'hirs_csrb_monthly_delivery_id',
                  'hirs_ctp_orbital_delivery_id', 'hirs_ctp_daily_delivery_id',
                  'hirs_ctp_monthly_delivery_id', 'hirs_tpw_orbital_delivery_id',
                  'hirs_tpw_daily_delivery_id']
    outputs = ['shift', 'noshift']

    def find_contexts(self, time_interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id,
                      hirs_csrb_daily_delivery_id, hirs_csrb_monthly_delivery_id,
                      hirs_ctp_orbital_delivery_id, hirs_ctp_daily_delivery_id,
                      hirs_ctp_monthly_delivery_id, hirs_tpw_orbital_delivery_id,
                      hirs_tpw_daily_delivery_id):

        granules = [g.left for g in time_interval.overlapping_interval_series(timedelta(days=1),
                                                                              timedelta(days=1))]

        LOG.debug("Running find_contexts()")
        return [{'granule': g,
                 'satellite': satellite,
                 'hirs2nc_delivery_id': hirs2nc_delivery_id,
                 'hirs_avhrr_delivery_id': hirs_avhrr_delivery_id,
                 'hirs_csrb_daily_delivery_id': hirs_csrb_daily_delivery_id,
                 'hirs_csrb_monthly_delivery_id': hirs_csrb_monthly_delivery_id,
                 'hirs_ctp_orbital_delivery_id': hirs_ctp_orbital_delivery_id,
                 'hirs_ctp_daily_delivery_id': hirs_ctp_daily_delivery_id,
                 'hirs_ctp_monthly_delivery_id': hirs_ctp_monthly_delivery_id,
                 'hirs_tpw_orbital_delivery_id': hirs_tpw_orbital_delivery_id,
                 'hirs_tpw_daily_delivery_id': hirs_tpw_daily_delivery_id}
                for g in granules]

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='HIRS_TPW_DAILY')
    def build_task(self, context, task):
        '''
        Build up a set of inputs for a single context
        '''
        global delta_catalog

        LOG.debug("Running build_task()")

        # Initialize the hirs_tpw_orbital module with the data locations
        hirs_tpw_orbital.delta_catalog = delta_catalog

        # Instantiate the hirs_tpw_orbital computation
        hirs_tpw_orbital_comp = hirs_tpw_orbital.HIRS_TPW_ORBITAL()

        SPC = StoredProductCatalog()

        # TPW Orbital Input

        granule = context['granule']
        wedge = timedelta(seconds=1)
        hour = timedelta(hours=1)
        day = timedelta(days=1)

        # Add an hour to each end of the day to make sure the day is completely covered
        interval = TimeInterval(context['granule'] - 1*hour, (context['granule'] + day + 1*hour))

        hirs_tpw_orbital_contexts = hirs_tpw_orbital_comp.find_contexts(
                                                                interval,
                                                                context['satellite'],
                                                                context['hirs2nc_delivery_id'],
                                                                context['hirs_avhrr_delivery_id'],
                                                                context['hirs_csrb_daily_delivery_id'],
                                                                context['hirs_csrb_monthly_delivery_id'],
                                                                context['hirs_ctp_orbital_delivery_id'],
                                                                context['hirs_ctp_daily_delivery_id'],
                                                                context['hirs_ctp_monthly_delivery_id'],
                                                                context['hirs_tpw_orbital_delivery_id']
                                                                )

        if len(hirs_tpw_orbital_contexts) == 0:
            raise WorkflowNotReady('No HIRS_TPW_ORBITAL inputs available for {}'.format(context['granule']))

        LOG.debug("There are {} TPW Orbital contexts for {}.".format(len(hirs_tpw_orbital_contexts), interval))

        for context in hirs_tpw_orbital_contexts:
            LOG.debug(context)

        # Knock off all but the last of the "previous" day's contexts
        this_day = granule.day
        previous_day = (granule - day + wedge).day
        next_day = (granule + day + wedge).day
        LOG.debug("previous_day: {}".format(previous_day))
        LOG.debug("this_day: {}".format(this_day))
        LOG.debug("next_day: {}".format(next_day))

        start_idx = 0
        end_idx = -1
        num_contexts = len(hirs_tpw_orbital_contexts)

        indices = np.arange(num_contexts)
        reverse_indices = np.flip(np.arange(num_contexts)-num_contexts, axis=0)

        # have this set to zero unless we need to set it otherwise (say for Metop-B)
        interval_pad = 0

        # Pruning all but the last of the previous day's contexts
        for idx in indices:
            if hirs_tpw_orbital_contexts[idx+interval_pad]['granule'].day == this_day:
                start_idx = idx
                LOG.debug("Breaking: start_idx = {}, granule = {}".format(
                    start_idx, hirs_tpw_orbital_contexts[start_idx]['granule']))
                break

        # Pruning all but the first of the next day's contexts
        for idx in reverse_indices:
            if hirs_tpw_orbital_contexts[idx-interval_pad]['granule'].day == this_day:
                end_idx = idx
                LOG.debug("Breaking: end_idx = {}, granule = {}".format(
                    end_idx, hirs_tpw_orbital_contexts[end_idx]['granule']))
                break

        hirs_tpw_orbital_contexts = hirs_tpw_orbital_contexts[start_idx:end_idx+1]
        #hirs_tpw_orbital_contexts = hirs_tpw_orbital_contexts[start_idx:end_idx]
        for context in hirs_tpw_orbital_contexts:
            LOG.debug("{}".format(context))

        for idx,context in enumerate(hirs_tpw_orbital_contexts):
            hirs_tpw_orbital_prod = hirs_tpw_orbital_comp.dataset('shift').product(context)
            if SPC.exists(hirs_tpw_orbital_prod):
                task.input('TPWO_shift-{}'.format(str(idx).zfill(2)), hirs_tpw_orbital_prod)

        for idx,context in enumerate(hirs_tpw_orbital_contexts):
            hirs_tpw_orbital_prod = hirs_tpw_orbital_comp.dataset('noshift').product(context)
            if SPC.exists(hirs_tpw_orbital_prod):
                task.input('TPWO_noshift-{}'.format(str(idx).zfill(2)), hirs_tpw_orbital_prod)

    def create_tpw_daily(self, inputs, context, shifted=False):

        '''
        Create the TPW statistics for the current day.
        '''

        rc = 0

        # Create the output directory
        current_dir = os.getcwd()

        # Get the required TPW script locations
        hirs_tpw_daily_delivery_id = context['hirs_tpw_daily_delivery_id']
        delivery = delivered_software.lookup('hirstpw_daily', delivery_id=hirs_tpw_daily_delivery_id)
        dist_root = pjoin(delivery.path, 'dist')
        version = delivery.version

        shifted_str = '_shift' if shifted else '_noshift'

        # Determine the output filenames
        output_file = 'hirs_tpw_daily_{}{}_{}.nc'.format(context['satellite'],
                                                          shifted_str,
                                                          context['granule'].strftime('D%y%j'))
        LOG.info("output_file: {}".format(output_file))

        # Generating TPW Orbital Input List
        tpw_orbital_file_list = 'tpw_orbital{}_list.txt'.format(shifted_str)
        with open(tpw_orbital_file_list, 'w') as f:
            [f.write('{}\n'.format(basename(inputs[key]))) for key in sorted(inputs.keys()) if shifted_str in key]

        # Run the TPW daily binary
        tpw_daily_bin = pjoin(dist_root, 'create_daily_daynight_tpw.exe')
        cmd = '{} {} {} {} {} {} &> {}'.format(
                tpw_daily_bin,
                tpw_orbital_file_list,
                output_file,
                context['granule'].strftime('%Y'),
                context['granule'].strftime('%m'),
                context['granule'].strftime('%j'),
                '{}.log'.format(splitext(output_file)[0])
                )
        #cmd = 'sleep 0.5; touch {}'.format(output_file)

        try:
            LOG.debug("cmd = \\\n\t{}".format(cmd.replace(' ',' \\\n\t')))
            rc_tpw = 0
            runscript(cmd, [delivery])
        except CalledProcessError as err:
            rc_tpw = err.returncode
            LOG.error(" TPW daily binary {} returned a value of {}".format(tpw_daily_bin, rc_tpw))
            return rc_tpw, None

        # Verify output file
        output_file = glob(output_file)
        if len(output_file) != 0:
            output_file = output_file[0]
            LOG.debug('Found output TPW daily file "{}"'.format(output_file))
        else:
            LOG.error('Failed to generate "{}", aborting'.format(output_file))
            rc = 1
            return rc, None

        return rc, output_file

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='HIRS_TPW_DAILY')
    def run_task(self, inputs, context):
        '''
        Run the TPW Daily binary on a single context
        '''

        LOG.debug("Running run_task()...")

        for key in context.keys():
            LOG.debug("run_task() context['{}'] = {}".format(key, context[key]))

        rc = 0

        # Link the inputs into the working directory
        inputs = symlink_inputs_to_working_dir(inputs)

        # Create the TPW daily file for the current day.
        rc, tpw_daily_noshift_file = self.create_tpw_daily(inputs, context, shifted=False)
        rc, tpw_daily_shift_file = self.create_tpw_daily(inputs, context, shifted=True)

        return {
                'noshift': nc_compress(tpw_daily_noshift_file),
                'shift': nc_compress(tpw_daily_shift_file)
                }
