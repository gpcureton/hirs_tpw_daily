from datetime import timedelta
import os
from flo.computation import Computation
from flo.subprocess import check_call
from flo.time import TimeInterval
from flo.util import augmented_env, symlink_inputs_to_working_dir
from flo.sw.hirs_tpw_orbital import HIRS_TPW_ORBITAL

# every module should have a LOG object
import logging, traceback
LOG = logging.getLogger(__file__)


class HIRS_TPW_DAILY(Computation):

    parameters = ['granule', 'sat', 'hirs_version', 'collo_version', 'csrb_version', 'ctp_version',
                  'tpw_version']
    outputs = ['out']

    def build_task(self, context, task):

        day = TimeInterval(context['granule'], (context['granule'] + timedelta(days=1) -
                                                timedelta(seconds=1)))

        tpw_orbital_contexts = HIRS_TPW_ORBITAL().find_contexts(context['sat'],
                                                                context['hirs_version'],
                                                                context['collo_version'],
                                                                context['csrb_version'],
                                                                context['ctp_version'],
                                                                context['tpw_version'], day)

        for (i, c) in enumerate(tpw_orbital_contexts):
            task.input('TPWO-{}'.format(i), HIRS_TPW_ORBITAL().dataset('out').product(c), True)

    def run_task(self, inputs, context):

        inputs = symlink_inputs_to_working_dir(inputs)
        lib_dir = os.path.join(self.package_root, context['tpw_version'], 'lib')

        output = 'tpw.daily.hirs.{}.{}.{}.ssec.nc'.format(context['sat'],
                                                          context['granule'].strftime('D%y%j'),
                                                          context['tpw_version'])

        # Generating TPW Orbital Input List
        tpw_orbital_file = 'tpw_orbital_list'
        with open(tpw_orbital_file, 'w') as f:
            [f.write('{}\n'.format(input)) for input in inputs.values()]

        cmd = os.path.join(self.package_root, context['tpw_version'],
                           'bin/create_daily_daynight_tpw.exe')
        cmd += ' {} {}'.format(tpw_orbital_file, output)

        print cmd
        check_call(cmd, shell=True, env=augmented_env({'LD_LIBRARY_PATH': lib_dir}))

        return {'out': output}

    def find_contexts(self, sat, hirs_version, collo_version, csrb_version, ctp_version,
                      tpw_version, time_interval):

        granules = [g.left for g in time_interval.overlapping_interval_series(timedelta(days=1),
                                                                              timedelta(days=1))]

        return [{'granule': g,
                 'sat': sat,
                 'hirs_version': hirs_version,
                 'collo_version': collo_version,
                 'csrb_version': csrb_version,
                 'ctp_version': ctp_version,
                 'tpw_version': tpw_version}
                for g in granules]

    def context_path(self, context, output):

        return os.path.join('HIRS',
                            '{}/{}'.format(context['sat'], context['granule'].year),
                            'TPW_DAILY')
