from setuptools import setup, find_packages

setup(
    name='cost_of_living_scraper',
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'requests',
        'beautifulsoup4',
        'pandas',
    ],
    entry_points={
        'console_scripts': [
            'cost_of_living_scraper=main:main',
        ],
    },
    description='A Python package to scrape cost of living data from Numbeo',
    author='Your Name',
    author_email='your.email@example.com',
)

