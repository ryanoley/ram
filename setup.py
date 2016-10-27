from distutils.core import setup

DISTNAME = 'ram'

MAINTAINER = 'RAM'
DESCRIPTION = 'Master System'

PACKAGES = ['ram',
            'ram/data',
            'ram/repository',
            'ram/strategy',
            'ram/strategy/benchmarks',
            'ram/strategy/gap',
            'ram/strategy/statarb',
            'ram/strategy/statarb/constructor',
            'ram/strategy/statarb/pairselector',
            'ram/strategy/vxx',
            'ram/utils']

setup(
    name=DISTNAME,
    version='0.1.0',
    packages=PACKAGES,
    maintainer=MAINTAINER,
    description=DESCRIPTION,
    long_description=open('README.md').read()
)
