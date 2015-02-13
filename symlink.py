from datetime import datetime
import os
from flo.config import config
from flo.product import StoredProductCatalog
from flo.time import TimeInterval
from flo.sw.hirs_tpw_daily import HIRS_TPW_DAILY


def symlink(c, output, contexts):

    SPC = StoredProductCatalog()

    for context in contexts:
        if SPC.exists(c.dataset(output).product(context)):
            s_path = os.path.join(config.get()['product_dir'],
                                  SPC.file(c.dataset(output).product(context)).path)
            d_path = os.path.join(config.get()['results_dir'],
                                  c.context_path(context, output))
            file_name = os.path.basename(s_path)

            if not os.path.exists(d_path):
                os.makedirs(d_path)

            if not os.path.isfile(os.path.join(d_path, file_name)):
                os.symlink(s_path, os.path.join(d_path, file_name))

output = 'out'
sat = 'metop-a'
hirs_version = 'v20140204'
collo_version = 'v20140204'
csrb_version = 'v20140204'
ctp_version = 'v20140204'
tpw_version = 'v20150212'
interval = TimeInterval(datetime(2009, 1, 1), datetime(2009, 2, 1))

c = HIRS_TPW_DAILY()
contexts = c.find_contexts(sat, hirs_version, collo_version, csrb_version, ctp_version,
                           tpw_version, interval)
symlink(c, output, contexts)
