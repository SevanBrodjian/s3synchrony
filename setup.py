import setuptools

with open('README.md', 'r') as fh:
      long_description = fh.read()

setuptools.setup(
      name='synchrony',
      version='0.1.0',
      description='This package provides a service for synchronizing file creations, deletions, and modifications across users on an AWS S3 prefix.',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Sevan Brodjian',
      author_email='sevanbro7@gmail.com',
      url='https://github.com/SevanBrodjian/s3synchrony',
      packages=setuptools.find_packages(),
      license='LICENSE.md',
     )