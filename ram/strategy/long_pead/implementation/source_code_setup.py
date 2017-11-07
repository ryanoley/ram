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
    'ram/strategy/long_pead',
    'ram/strategy/long_pead/constructor',
    'ram/strategy/long_pead/data',
    'ram/strategy/long_pead/signals',
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
