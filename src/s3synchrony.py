"""Perform functionality for synchronizing and resetting with a repo.

get_supported_platforms - Get a list of all data platforms supported currently
smart_sync - Synchronizes all local files with a remote repository through user input
reset_all - Completely resets both local and remote repos with user input confirmation

  Typical usage example (for S3):

  import s3synchrony as s3s
  params = {}
  params["datafolder"] = "Data"
  params["aws_bkt"] = "analytics_development"
  params["aws_prfx"] = "S3_Synchrony_Testing"
  if(len(sys.argv) > 1 and sys.argv[1] == "reset"):
      s3s.reset_all(**params)
  else:
      s3s.smart_sync(**params)

Copyright (C) 2022  Sevan Brodjian
Created for Ameren at the Ameren Innovation Center @ UIUC

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

from DataPlatforms import baseconn
from DataPlatforms import s3conn


_supported_platforms = {"S3": s3conn.S3Connection}


def get_supported_platforms():
    """Return a list of the supported data platforms."""
    return [*_supported_platforms]


def smart_sync(platform="S3", **kwargs):
    """Perform all necessary steps to synchronize a local repository with a remote repo.

    Notes:
    Keyword arguments are dependent on platform selection.
    """

    if(platform in _supported_platforms):
        connection = _supported_platforms[platform](**kwargs)
    else:
        connection = baseconn.DataPlatformConnection(**kwargs)

    connection.intro_message()
    connection.establish_connection()
    connection.synchronize()
    connection.close_message()


def reset_all(platform="S3", **kwargs):
    """Reset local and remote directories to original state.

    Notes:
    Keyword arguments are dependent on platform selection.
    """

    if(platform in _supported_platforms):
        connection = _supported_platforms[platform](**kwargs)
    else:
        connection = baseconn.DataPlatformConnection(**kwargs)

    connection.intro_message()
    connection.establish_connection()
    if connection.reset_confirm():
        connection.reset_local()
        connection.reset_remote()
    connection.close_message()
