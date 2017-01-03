#!/usr/bin/env python

import re

import codecs

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

packages = [
    'stimpi',
    'stimpi.adapters',
    'stimpi.frames',
    'stimpi.frames.spec',
    'stimpi.frames.impl'
]

requires = []
with codecs.open('requirements/production.txt', 'r') as instream:
    for line in instream:
        requires.append(line.strip())

version = ''
with codecs.open('stimpi/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

if not version:
    raise RuntimeError('Cannot find version information')

with codecs.open('README.rst', 'r', 'utf-8') as f:
    readme = f.read()

setup(
    name='stimpi',
    version=version,
    description='STOMP 1.0+ Asynchronous Client',
    long_description=readme,
    author='Geoff MacGill',
    author_email='skippydev007@gmail.com',
    url='https://github.com/skippyprime/stimpi',
    packages=packages,
    package_data={'': ['LICENSE', ]},
    package_dir={'stimpi': 'stimpi'},
    include_package_data=True,
    install_requires=requires,
    license='Apache 2.0',
    zip_safe=False,
    classifiers=(
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7'
    ),
    extras_require={
    },
)
