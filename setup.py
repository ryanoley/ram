from distutils.core import setup

DISTNAME = 'ram'

MAINTAINER = 'RAM'
DESCRIPTION = 'Master System'

PACKAGES = ['ram',
            'ram/analysis',
            'ram/aws',
            'ram/data',
            'ram/portfolio_agent',
            'ram/strategy',
            'ram/strategy/birds',
            'ram/strategy/birds/constructor',
            'ram/strategy/birds/signals',
            'ram/strategy/gap',
            'ram/strategy/statarb',
            'ram/strategy/statarb/constructor',
            'ram/strategy/statarb/pairselector',
            'ram/strategy/yearend',
            'ram/strategy_repo',
            'ram/strategy_repo/basics',
            'ram/utils']

setup(
    name=DISTNAME,
    version='0.1.0',
    packages=PACKAGES,
    maintainer=MAINTAINER,
    description=DESCRIPTION,
    long_description=open('README.md').read()
)
