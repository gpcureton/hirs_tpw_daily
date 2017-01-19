import os
from datetime import datetime, timedelta
import logging
import traceback

from flo.time import TimeInterval
from flo.ui import local_prepare, local_execute

from flo.sw.hirs_tpw_orbital import HIRS_TPW_ORBITAL
from flo.sw.hirs_tpw_daily import HIRS_TPW_DAILY

# every module should have a LOG object
LOG = logging.getLogger(__file__)

# Set up the logging
levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
level = levels[3]
if level == logging.DEBUG:
    console_logFormat = '%(asctime)s.%(msecs)03d (%(levelname)s) : %(filename)s : %(funcName)s : %(lineno)d:%(message)s'
    dateFormat = '%Y-%m-%d %H:%M:%S'
else:
    console_logFormat = '%(asctime)s.%(msecs)03d (%(levelname)s) : %(message)s'
    dateFormat = '%Y-%m-%d %H:%M:%S'

logging.basicConfig(level=levels[2],
                    format=console_logFormat,
                    datefmt=dateFormat)

# General information
comp = HIRS_TPW_DAILY()

#
# Local execution
#

def local_execute_example(granule, platform, hirs_version, collo_version, csrb_version, ctp_version,
                          tpw_version, skip_prepare=False, skip_execute=False):
    comp_dict = { 
        'granule': granule
        'sat': platform, 
        'hirs_version': hirs_version, 
        'collo_version': collo_version, 
        'csrb_version': csrb_version,
        'ctp_version': ctp_version,
        'tpw_version': tpw_version}

    try:
        if not skip_prepare:
            LOG.info("Running local prepare...")
            local_prepare(comp, comp_dict, download_only=[HIRS_TPW_ORBITAL()])
        if not skip_execute:
            LOG.info("Running local execute...")
            local_execute(comp, comp_dict)
    except Exception, err:
        LOG.error("{}".format(err))
        LOG.info(traceback.format_exc())

def print_contexts(platform, dt_left, dt_right, granule_length):
    interval = TimeInterval(dt_left, dt_right)
    contexts = comp.find_contexts(platform, hirs_version, collo_version, csrb_version, ctp_version,
                                  tpw_version, interval)
    contexts.sort()
    for context in contexts:
        print context

    return contexts

platform_choices = ['noaa-06', 'noaa-07', 'noaa-08', 'noaa-09', 'noaa-10', 'noaa-11',
                    'noaa-12', 'noaa-14', 'noaa-15', 'noaa-16', 'noaa-17', 'noaa-18',
                    'noaa-19', 'metop-a', 'metop-b']

platform = 'metop-b'
hirs_version  = 'v20151014'
collo_version = 'v20151014'
csrb_version  = 'v20150915'
ctp_version = 'v20150915'
tpw_version = 'v20160222'
granule = datetime(2016, 6, 3, 21, 17)
granule = datetime(2016, 6, 3, 20, 32)

#local_execute_example(sat, hirs_version, collo_version, csrb_version, granule)
