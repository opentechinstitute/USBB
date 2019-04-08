import sys
import os
import logging
import subprocess
import pickle
import math
import apache_beam as beam

import setuptools
import distutils

from setuptools.command.install import install as _install



class install(_install):  # pylint: disable=invalid-name
    def run(self):
        self.run_command('CustomCommands')
        _install.run(self)

CUSTOM_COMMANDS = [
    ['pip', 'install','apache_beam==2.11.0'],
]


class CustomCommands(setuptools.Command):
    """A setuptools Command class able to run arbitrary commands."""

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def RunCustomCommand(self, command_list):
        logging.info('Running command: %s' % command_list)
        p = subprocess.Popen(
            command_list,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        # Can use communicate(input='y\n'.encode()) if the command run requires
        # some confirmation.
        stdout_data, _ = p.communicate()
        logging.info('Command output: %s' % stdout_data)
        if p.returncode != 0:
            raise RuntimeError(
                'Command %s failed: exit code: %s' % (command_list, p.returncode))

    def run(self):
        for command in CUSTOM_COMMANDS:
            self.RunCustomCommand(command)
 

REQUIRED_PACKAGES = [
    'matplotlib==2.2.2',
    'apache_beam==2.11.0',
]


setuptools.setup(
    name='name',
    version='1.0.0',
    description='DataFlow worker',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    cmdclass={
        'install': install,
        'CustomCommands': CustomCommands,
        }
    )
