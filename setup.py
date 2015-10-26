import re
import os.path
import sys
from setuptools import setup, find_packages


install_requires = []

PY_VER = sys.version_info

if PY_VER >= (3, 4):
    pass
elif PY_VER >= (3, 3):
    install_requires.append('asyncio')
else:
    raise RuntimeError("aionsq doesn't support Python version prior 3.3")


def read(*parts):
    with open(os.path.join(*parts), 'rt') as f:
        return f.read().strip()


def read_version():
    regexp = re.compile(r"^__version__\W*=\W*'([\d.abrc]+)'")
    init_py = os.path.join(os.path.dirname(__file__),
                           'aionsq', '__init__.py')
    with open(init_py) as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        else:
            raise RuntimeError('Cannot find version in aionsq/__init__.py')


classifiers = [
    'License :: OSI Approved :: MIT License',
    'Development Status :: 4 - Beta',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Operating System :: POSIX',
    'Environment :: Web Environment',
    'Intended Audience :: Developers',
    'Topic :: Software Development',
    'Topic :: Software Development :: Libraries',
]

setup(name='aionsq',
      version=read_version(),
      description=("asyncio (PEP 3156) nsq support"),
      long_description=read('README.rst'),
      classifiers=classifiers,
      platforms=["POSIX"],
      author="Nikolay Novik",
      author_email="nickolainovik@gmail.com",
      url="https://github.com/jettify/aionsq",
      license="MIT",
      packages=find_packages(exclude=["tests"]),
      install_requires=install_requires,
      include_package_data=True,
      )