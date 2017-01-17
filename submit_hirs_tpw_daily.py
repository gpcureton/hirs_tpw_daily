import os
import sys
import time
import re
import string
from datetime import datetime, timedelta
from calendar import monthrange
import logging
import traceback

from subprocess import CalledProcessError, call
from subprocess import Popen, STDOUT, PIPE

from flo.time import TimeInterval
from flo.ui import safe_submit_order
from flo.product import StoredProductCatalog

from flo.sw.hirs_tpw_orbital import HIRS_TPW_ORBITAL
from flo.sw.hirs_tpw_daily import HIRS_TPW_DAILY

# every module should have a LOG object
LOG = logging.getLogger(__file__)

# Set up the logging
console_logFormat = '%(asctime)s : (%(levelname)s):%(filename)s:%(funcName)s:%(lineno)d:  %(message)s'
dateFormat = '%Y-%m-%d %H:%M:%S'
levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
logging.basicConfig(stream=sys.stdout, level=levels[0],
                    format=console_logFormat,
                    datefmt=dateFormat)


# General information
comp = HIRS_TPW_DAILY()
SPC = StoredProductCatalog()

# Latest Computation versions.
hirs_version  = 'v20151014'
collo_version = 'v20151014'
csrb_version  = 'v20150915'
ctp_version   = 'v20150915'
tpw_version   = 'v20160222'

platform_choices = ['noaa-06', 'noaa-07', 'noaa-08', 'noaa-09', 'noaa-10', 'noaa-11',
                    'noaa-12', 'noaa-14', 'noaa-15', 'noaa-16', 'noaa-17', 'noaa-18',
                    'noaa-19', 'metop-a', 'metop-b']

platform = 'metop-b'

# Specify the intervals
wedge = timedelta(seconds=1.)
#intervals = [
    #TimeInterval(datetime(2016, 1, 1), datetime(2016, 2, 1) - wedge),
    #TimeInterval(datetime(2016, 2, 1), datetime(2016, 3, 1) - wedge),
    #TimeInterval(datetime(2016, 3, 1), datetime(2016, 4, 1) - wedge),
    #TimeInterval(datetime(2016, 4, 1), datetime(2016, 5, 1) - wedge),
    #TimeInterval(datetime(2016, 5, 1), datetime(2016, 6, 1) - wedge),
    #TimeInterval(datetime(2016, 6, 1), datetime(2016, 7, 1) - wedge),
    #TimeInterval(datetime(2016, 7, 1), datetime(2016, 8, 1) - wedge),
    #TimeInterval(datetime(2016, 8, 1), datetime(2016, 9, 1) - wedge),
    #TimeInterval(datetime(2016, 9, 1), datetime(2016, 10, 1) - wedge),
    #TimeInterval(datetime(2016, 10, 1),datetime(2016, 11, 1) - wedge),
    #TimeInterval(datetime(2016, 11, 1),datetime(2016, 12, 1) - wedge),
    #TimeInterval(datetime(2016, 12, 1),datetime(2017, 1, 1) - wedge),
#]

intervals = []
year,month = 2016,5
months = range(1, 12 + 1)
for month in months:
    days_in_month = monthrange(year, month)[1]
    dt_start = datetime(year, month, 1)
    dt_end = datetime(year, month, 1) + timedelta(days = days_in_month)
    interval = TimeInterval(dt_start, dt_end - wedge)
    contexts = comp.find_contexts(platform, hirs_version, collo_version, csrb_version, ctp_version,
                               tpw_version, interval)
    num_contexts_exist = 0
    for context in contexts:
        num_contexts_exist += SPC.exists(comp.dataset('means').product(context))
    LOG.info("Interval {} has {}/{} contexts existing".format(interval, num_contexts_exist, len(contexts)))
    missing_contexts = len(contexts) - num_contexts_exist
    if missing_contexts > 3:
        intervals.append(interval)

LOG.info("Submitting intervals...")
for interval in intervals:
    LOG.info("Submitting interval {} -> {}".format(interval.left, interval.right))
    contexts = comp.find_contexts(platform, hirs_version, collo_version, csrb_version, ctp_version,
                               tpw_version, interval)
    LOG.info("\tThere are {} contexts in this interval".format(len(contexts)))
    contexts.sort()
    #for context in contexts:
        #LOG.debug(context)
    LOG.info("\tFirst context: {}".format(contexts[0]))
    LOG.info("\tLast context:  {}".format(contexts[-1]))
    LOG.info("\t{}".format(safe_submit_order(comp,
                                             [comp.dataset('out')],
                                             contexts,
                                             download_onlies=[HIRS_TPW_ORBITAL()])))
