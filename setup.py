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
    'ram/strategy/basic',
    'ram/strategy/basic/data',
    'ram/strategy/basic/constructor',
    'ram/strategy/basic/signals',
    'ram/strategy/cta',
    'ram/strategy/etfs',
    'ram/strategy/etfs/src',
    'ram/strategy/gap',
    'ram/strategy/intraday_reversion',
    'ram/strategy/intraday_reversion/src',
    'ram/strategy/long_pead',
    'ram/strategy/long_pead/constructor',
    'ram/strategy/long_pead/data',
    'ram/strategy/long_pead/signals',
    'ram/strategy/momentum',
    'ram/strategy/reversion',
    'ram/strategy/vxx',
    'ram/strategy/yearend',
    'ram/utils',
    
    'ram/strategy/starmine',
    'ram/strategy/starmine/constructor',
    'ram/strategy/starmine/data',
    'ram/strategy/starmine/signals',
]

setup(
    name=DISTNAME,
    version='0.1.0',
    packages=PACKAGES,
    maintainer=MAINTAINER,
    description=DESCRIPTION,
    long_description=open('README.md').read()
)
