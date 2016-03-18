import sys
from setuptools import setup, find_packages

INSTALL_REQUIRES = ['enerdata']

if sys.version_info < (2, 7):
    INSTALL_REQUIRES += ['backport_collections']

setup(
    name='oraKWlum',
    version='0.0.1',
    packages=find_packages(),
    url='http://code.gisce.net',
    license='GPL3',
    author='GISCE-TI, S.L.',
    author_email='devel@gisce.net',
    install_requires=INSTALL_REQUIRES,
    description=''
)
