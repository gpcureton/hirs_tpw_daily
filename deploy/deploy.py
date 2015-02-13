from flo_deploy.packagelib import *


class HIRS_TPW_DAILY_Package(Package):

    def deploy_package(self):

        for version in ['v20150212']:
            self.merge(Extracted('HIRS_TPW_Daily_{}.tar.gz'.format(version)).path(), version)
            self.merge(NetcdfFortran().path(), version)
            self.merge(Netcdf().path(), version)
            self.merge(Hdf5().path(), version)
