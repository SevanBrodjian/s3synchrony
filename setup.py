import setuptools

with open('README.md', 'r') as fh:
      long_description = fh.read()

setuptools.setup(
      name='s3synchrony',
      version='0.1.0',
      description='S3 Synchronization Service',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Sevan Brodjian',
      author_email='sevanbro7@gmail.com',
      url='https://github.com/SevanBrodjian/s3synchrony',
      packages=setuptools.find_packages(),
      license='LICENSE.md',
     )