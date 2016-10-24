from distutils.core import setup

DISTNAME = 'platform'

MAINTAINER = 'RAM'
DESCRIPTION = 'Master System'

PACKAGES = ['platform',
            'platform/data',
            'platform/repository',
            'platform/strategy',
            'platform/strategy/vxx']

setup(
    name=DISTNAME,
    version='0.1.0',
    packages=PACKAGES,
    maintainer=MAINTAINER,
    description=DESCRIPTION,
    long_description=open('README.md').read()
)
