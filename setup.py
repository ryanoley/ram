from distutils.core import setup

DISTNAME = 'ram'

MAINTAINER = 'RAM'
DESCRIPTION = 'Master System'

PACKAGES = [
    'ram',
    'ram/analysis',
    'ram/analysis/model_selection',
    'ram/aws',
    'ram/data',
    'ram/strategy',
    # ~~~ Intraday Reversion ~~~
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
    'ram/strategy/statarb/objects',
    'ram/strategy/statarb/implementation',
    'ram/strategy/statarb/implementation/tests',
    'ram/strategy/statarb/version_004',
    'ram/strategy/statarb/version_004/constructor',
    'ram/strategy/statarb/version_004/data',
    'ram/strategy/statarb/version_004/signals',
    'ram/strategy/statarb/version_005',
    'ram/strategy/statarb/version_005/constructor',
    'ram/strategy/statarb/version_005/data',
    'ram/strategy/statarb/version_005/signals',
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
