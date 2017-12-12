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
