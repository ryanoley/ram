from distutils.core import setup

DISTNAME = 'ram'

MAINTAINER = 'RAM'
DESCRIPTION = 'Master System'

PACKAGES = ['ram',
            'ram/analysis',
            'ram/aws',
            'ram/data',
            'ram/repository',
            'ram/strategy',
            'ram/strategy/benchmarks',
            'ram/strategy/birds',
            'ram/strategy/gap',
            'ram/strategy/reversion',
            'ram/strategy/statarb',
            'ram/strategy/statarb/constructor',
            'ram/strategy/statarb/pairselector',
            'ram/strategy/vxx',
            'ram/strategy/yearend',
            'ram/utils']

setup(
    name=DISTNAME,
    version='0.1.0',
    packages=PACKAGES,
    maintainer=MAINTAINER,
    description=DESCRIPTION,
    long_description=open('README.md').read()
)
