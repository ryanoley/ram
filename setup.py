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
    'ram/strategy/cta',
    'ram/strategy/etfs',
    'ram/strategy/gap',
    'ram/strategy/long_pead',
    'ram/strategy/momentum',
    'ram/strategy/reversion',
    'ram/strategy/statarb',
    'ram/strategy/statarb/constructor',
    'ram/strategy/statarb/pairselector',
    'ram/strategy/statarb/responses',
    'ram/strategy/vxx',
    'ram/strategy/yearend',
    'ram/strategy_repo',
    'ram/strategy_repo/basics',
    'ram/utils'
]

setup(
    name=DISTNAME,
    version='0.1.0',
    packages=PACKAGES,
    maintainer=MAINTAINER,
    description=DESCRIPTION,
    long_description=open('README.md').read()
)
