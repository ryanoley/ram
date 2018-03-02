from distutils.core import setup

DISTNAME = 'ram'

MAINTAINER = 'RAM'
DESCRIPTION = 'Master System'

PACKAGES = [
    'ram',
    'ram/analysis',
    'ram/aws',
    'ram/data',
    'ram/strategy',
    'ram/strategy/intraday_reversion',
    'ram/strategy/intraday_reversion/src',
    # ~~~ Analyst Estimates ~~~
    'ram/strategy/analyst_estimates',
    'ram/strategy/analyst_estimates/base',
    'ram/strategy/analyst_estimates/version_001',
    'ram/strategy/analyst_estimates/version_001/constructor',
    'ram/strategy/analyst_estimates/version_001/data',
    'ram/strategy/analyst_estimates/version_001/signals',
    # ~~~ STATARB ~~~
    'ram/strategy/statarb',
    'ram/strategy/statarb/abstract',
    'ram/strategy/statarb/implementation',
    'ram/strategy/statarb/version_001',
    'ram/strategy/statarb/version_001/constructor',
    'ram/strategy/statarb/version_001/data',
    'ram/strategy/statarb/version_001/signals',
    'ram/strategy/statarb/version_002',
    'ram/strategy/statarb/version_002/constructor',
    'ram/strategy/statarb/version_002/data',
    'ram/strategy/statarb/version_002/signals',

    # ~~~ STATARB2 ~~~
    'ram/strategy/statarb2',
    'ram/strategy/statarb2/implementation',
    'ram/strategy/statarb2/version_001',
    'ram/strategy/statarb2/version_002',
    'ram/strategy/statarb2/version_003',
    'ram/strategy/statarb2/version_004',
    'ram/strategy/statarb2/version_005',
    'ram/strategy/statarb2/version_006',
    # ~~~~~~~~~~~~~~~
    'ram/utils',
]

setup(
    name=DISTNAME,
    version='0.1.0',
    packages=PACKAGES,
    maintainer=MAINTAINER,
    description=DESCRIPTION,
    long_description=open('README.md').read()
)
