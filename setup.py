from setuptools import setup, find_packages

with open("requirements.txt") as fd:
    dependencies = [dep for dep in fd.readlines()]

setup(
    name='manalink',
    version='0.3.0',
    author='Graham Keenan',
    author_email='graham.keenan@outlook.com',
    description='Expandable P2P communication protocols',
    install_requires=dependencies,
    packages=find_packages(),
    python_requires='>=3.7'
)
