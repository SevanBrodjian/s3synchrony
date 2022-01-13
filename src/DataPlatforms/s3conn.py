"""Contains the S3Connection class.

S3Connection - Data platform class for synchronizing with AWS S3.

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

import os
import sys
import pathlib

import pandas as pd
import datetime as dt
import boto3
import botocore.exceptions
import pyperclip
import shutil

from DataPlatforms import baseconn


class S3Connection(baseconn.DataPlatformConnection):
    """Data platform class for synchronizing with AWS S3.

    An AWS S3 prefix does not need to be created already for synchronization, nor does
    the local data folder need to be made. The only thing required are your AWS credentials.

    Attributes:
        columns - A list of the column names used for synchronization.
        dttm_format - A string for the format being used for all datetime formatting.
        datafolder - Name of the local datafolder being used to store data.
        aws_bkt - The AWS bucket name for synchronization.
        aws_prfx - The AWS prefix which we are synchronizing with.
    """

    # pylint: disable=too-many-instance-attributes
    # Different datafolder names for different S3 instances leads to
    # a large number of instance variables.

    _file_colname = "File"
    _editor_colname = "Edited By"
    _time_colname = "Time Edited"
    _hash_colname = "Checksum"
    _s3id = ".S3"
    columns = [_file_colname, _editor_colname, _time_colname, _hash_colname]
    dttm_format = "%Y-%m-%d %H:%M:%S"

    def __init__(self, **kwargs):
        """Initialize all necessary instance variables.

        Args:
            datafolder: The name of the local folder used to store data.
            aws_bkt: The name of the S3 bucket where the remote repo will be stored.
            aws_prfx: The prefix to the S3 location of the remote repo.

        Returns:
            None.
        """

        super().__init__()
        self.datafolder = "Data" if "datafolder" not in kwargs else kwargs["datafolder"]
        self.aws_bkt = None if "aws_bkt" not in kwargs else kwargs["aws_bkt"]
        self.aws_prfx = None if "aws_prfx" not in kwargs else kwargs["aws_prfx"]
        self._s3subdirlocal = self.datafolder + '/' + self._s3id
        self._s3subdirremote = self.aws_prfx + '/' + self._s3id + '/'
        self._s3versionspath = self._s3subdirlocal + "/versions.csv"
        self._localversionspath = self._s3subdirlocal + "/versionsLocal.csv"
        self._s3delpath = self._s3subdirlocal + "/deletedS3.csv"
        self._localdelpath = self._s3subdirlocal + "/deletedLocal.csv"
        self._tmppath = self._s3subdirlocal + "/tmp"
        self._logspath = self._s3subdirlocal + "/logs"
        self._ignorepath = self._s3subdirlocal + "/ignores3.txt"

        self._ignore = []
        self._logname = dt.datetime.now().strftime("%Y_%m_%d_%H_%M_%S_")
        self._logname += self._get_randomized_dirname()[10:]
        self._logname = self._logspath + '/' + self._logname + ".txt"
        self._log = ""
        self._usernamepath = self._s3subdirlocal + "/user_name.txt"
        self._awscredspath = self._s3subdirlocal + "/aws.txt"
        self._name = "NONAME"
        self._reset_approved = False

    def establish_connection(self):
        """Forms a connection to S3 for synchronization or resetting.

        Currently, the connection is supported only through directly supplying credentials.
        This is done via the pyperclip library. Through the AWS "Command line or programmatic
        access" your credentials will be stored to the clipboard, where they are then parsed
        and loaded by this function.

        Your credentials will only need to be updated as often as they are changed, and do not
        need to be stored to the clipboard every time this function is called.

        Args:
            None.

        Returns:
            None.

        Side Effects:
            Credentials will be stored in a local text file, and updated if necessary. An
            S3 connection is then attempted to be formed.

        Raises:
            Botocore exceptions can occur for invalid credentials or other connection issues.
        """

        # Check that the data folder has been created on our local system
        if(not os.path.exists(self.datafolder)):
            print("No " + self.datafolder +
                  " directory present, creating new one...")
            os.mkdir(self.datafolder)
            print("Done.\n")
        if(not os.path.exists(self._s3subdirlocal)):
            os.mkdir(self._s3subdirlocal)

        subfolders = self._connect_to_s3()

        # Check if there is a .S3 subfolder in this S3 bucket/prefix
        if(self._s3subdirremote not in subfolders):
            print("This S3 prefix has not been initialized for S3 Synchrony - Initializing prefix and uploading to S3...")
            self._initialize_prefix()
            print("Done.\n")

        # Check if our data folder has all the necessary csvs and subdirectories.
        # If it doesn't then we need to overwrite the S3 folder with all new info
        has_local_csvs = (os.path.exists(self._localversionspath)
                          and os.path.exists(self._localdelpath))
        has_s3_csvs = (os.path.exists(self._s3versionspath)
                       and os.path.exists(self._s3delpath))
        if(not has_local_csvs or not has_s3_csvs):
            print(
                "Your data folder has not been initialized for S3 Synchrony - Downloading from S3...")
            self._download_entire_prefix(
                self.aws_bkt, self._s3subdirremote, self._s3subdirlocal)
            empty = pd.DataFrame(columns=self.columns)
            empty.to_csv(self._localdelpath, index=False)
            empty.to_csv(self._localversionspath, index=False)
            print("Done.\n")

        if(not os.path.exists(self._tmppath)):
            os.mkdir(self._tmppath)
        if(not os.path.exists(self._logspath)):
            os.mkdir(self._logspath)
        if(not os.path.exists(self._ignorepath)):
            with open(self._ignorepath, 'w') as f:
                f.write("")

        with open(self._ignorepath) as f:
            lines = f.readlines()
            self._ignore = [line.rstrip() for line in lines]
        with open(self._logname, 'w') as f:
            f.write("")

    def intro_message(self):
        """Print an introductory message to signal program start."""
        print()
        print("############################")
        print("#      AWS Smart Sync      #")
        print("############################")
        print()

    def close_message(self):
        """Print a closing message to signal program end."""
        print()
        print("############################")
        print("#       Done Syncing       #")
        print("############################")

    def synchronize(self):
        """Synchronize local repository with the S3 prefix.

        The user will be prompted to download or upload all file changes including new files,
        deletions, and modified files. Modifications are determined via checksums, and additional
        file information will be printed to help the user.

        Args:
            None.

        Returns:
            None.

        Side Effects:
            Modifies local files and remote files on S3 prefix.
        """

        self._download_file(self.aws_bkt, self._s3subdirremote +
                            "versions.csv", self._s3versionspath)
        self._download_file(
            self.aws_bkt, self._s3subdirremote + "deletedS3.csv", self._s3delpath)

        self._push_deleted_s3()
        self._pull_deleted_local()

        self._push_new_s3()
        self._pull_new_local()

        self._push_modified_s3()
        self._pull_modified_local()

        self._revert_modified_s3()
        self._revert_modified_local()

        self.resource.meta.client.upload_file(
            self._s3versionspath, self.aws_bkt, self._s3subdirremote + "versions.csv")
        self.resource.meta.client.upload_file(
            self._s3delpath, self.aws_bkt, self._s3subdirremote + "deletedS3.csv")

        # Save a snapshot of our current files into versionsLocal for next time
        self._compute_directory(self.datafolder).to_csv(
            self._localversionspath, index=False)

        if(self._log == ""):
            os.remove(self._logname)
        else:
            with open(self._logname, 'w') as f:
                f.write(self._log)

    def reset_confirm(self):
        """Prompt the user to confirm whether a reset can occur.

        Args:
            None.

        Returns:
            A boolean containing the user's decision.

        Side Effects:
            Saves the user's decision in a private instance variable to allow a reset later.
        """

        print("Are you sure you would like to reset S3?")
        print("This will not change any of your file contents, but will delete the entire")
        confirm = input(
            ".S3 folder on your local computer and on your AWS prefix: " + self.aws_prfx + " (y/n): ")

        if(confirm.lower() not in ['y', "yes"]):
            print("\nReset aborted.")
            self._reset_approved = False
            return False
        self._reset_approved = True
        return True

    def reset_local(self):
        """Remove all signs of synchronization from local directory.

        Args:
            None.

        Returns:
            None.

        Side Effects:
            Deletes the .S3 folder within the data folder if the user approved a reset.
        """

        if self._reset_approved:
            print("\nDeleting " + self._s3subdirlocal)
            shutil.rmtree(self._s3subdirlocal)
            print("Done.")
        else:
            print("Cannot reset local -- user has not approved.")

    def reset_remote(self):
        """Remove all signs of synchronization from remote S3 prefix.

        Args:
            None.

        Returns:
            None.

        Side Effects:
            Deletes the .S3 folder from the S3 prefix if the user approved a reset.
        """

        if self._reset_approved:
            print("\nDeleting " + self._s3subdirremote)
            response = self.client.list_objects_v2(
                Bucket=self.aws_bkt, Prefix=self._s3subdirremote)
            for file_dict in response["Contents"]:
                self.client.delete_object(
                    Bucket=self.aws_bkt, Key=file_dict["Key"])
            print("Done.")
        else:
            print("Cannot reset remote -- user has not approved.")

    def _import_credentials(self, credentials_path, role_requested):
        """Read the user's credentials from the text file containing them."""
        with open(credentials_path, 'r') as file:
            roles = {}
            for line in file.readlines():
                line = line.strip()
                if line[0] == '[' and line[-1] == ']':
                    role = line[1:-1]
                    roles[role] = {}
                elif line == '':
                    continue
                else:
                    key_value_list = line.split('=')
                    if len(key_value_list) != 1:
                        for i in range(len(line)):
                            if line[i] == '=':
                                key = line[:i].strip()
                                value = line[(i+1):].strip()
                                break
                        roles[role].update({key: value})

        role_dict = {}
        for role_id in roles:
            if role_requested in role_id:
                role_dict = roles[role_id]

        self.credentials = role_dict
        self.resource = boto3.resource("s3", **self.credentials)
        self.client = boto3.client("s3", **self.credentials)

    def _connect_to_s3(self):
        """Check for necessary files and attempt to connect with S3 by validating credentials."""
        if(not os.path.exists(self._usernamepath)):
            user_name = input("First Time Setup - please type your name: ")
            if(user_name == ""):
                user_name = "NONAME"

            with open(self._usernamepath, 'w') as f:
                f.write(user_name)

        if(not os.path.exists(self._awscredspath)):
            print("No aws_creds.txt file found, creating one and trying to grab credentials from your clipboard...")
            with open(self._awscredspath, "w") as f:
                f.write("")
            if(self._update_aws_creds(self._awscredspath)):
                print("Successfully grabbed your AWS credentials from the clipboard.\n")
            else:
                print(
                    "\nFAILED: Please make sure you have your AWS credentials saved to your clipboard.")
                print(
                    "Please refer to the documentation if you are unsure how to do this.\n")
                quit()

        with open(self._usernamepath, 'r') as f:
            first_name = f.readline()
        if(first_name == ""):
            first_name = "NONAME"
        with open(self._awscredspath, 'r') as f:
            aws_role = f.readline()
        if(aws_role == ""):
            aws_role = "NONE"
            print("No AWS Role Found.\n")
        else:
            aws_role = aws_role[1:-2]

        self._name = first_name
        self._import_credentials(self._awscredspath, aws_role)

        print("Checking credentials - attempting S3 connection...")
        try:
            objects = self.client.list_objects(
                Bucket=self.aws_bkt, Prefix=self.aws_prfx + '/', Delimiter='/')
        except (botocore.exceptions.ClientError, botocore.exceptions.NoCredentialsError) as exc:
            if("Access Denied" in str(exc)):
                print(
                    "ERROR: ACESS DENIED. Do you have the right role? Attempting to update credentials from clipboard...")
            else:
                print(
                    "ERROR: INVALID CREDENTIALS. Are they expired? Attempting to update credentials from clipboard...")

            with open(self._awscredspath, 'w') as f:
                f.write('')
            if (self._update_aws_creds(self._awscredspath)):
                print("\nSUCCESS: AWS Credentials successfully updated from clipboard.")
                print("Reattempting S3 connection...\n")

                with open(self._awscredspath, 'r') as f:
                    aws_role = f.readline()
                if(aws_role == ""):
                    aws_role = "NONE"
                    print("No AWS Role Found.\n")
                else:
                    aws_role = aws_role[1:-2]
                    print("Found AWS Role: " + aws_role + "\n")

                try:
                    self._import_credentials(self._awscredspath, aws_role)
                    objects = self.client.list_objects(
                        Bucket=self.aws_bkt, Prefix=self.aws_prfx + '/', Delimiter='/')
                except Exception as exc:
                    print("FAILED. Could not connect to S3. Error message:\n")
                    print(exc)
                    quit()
            else:
                print(
                    "\nFAILED: Please make sure you have your AWS credentials saved to your clipboard.")
                print(
                    "Please refer to the documentation if you are unsure how to do this.\n")
                quit()

        subfolders = []
        try:
            for prefix in objects.get("CommonPrefixes"):
                subfolders.append(prefix.get("Prefix"))
        except:
            pass
        print("Successfully connected to S3.\n")
        return subfolders

    def _initialize_prefix(self):
        """Check for all necessary files on the S3 prefix for synchronization."""
        randhex = self._get_randomized_dirname()
        self._download_entire_prefix(
            self.aws_bkt, self.aws_prfx + '/', self._tmppath + '/' + randhex)
        versions = self._compute_directory(
            self._tmppath + '/' + randhex, False)

        if(not os.path.exists(self.datafolder)):
            os.mkdir(self.datafolder)
        if(not os.path.exists(self._s3subdirlocal)):
            os.mkdir(self._s3subdirlocal)
        if(not os.path.exists(self._tmppath)):
            os.mkdir(self._tmppath)
        if(not os.path.exists(self._tmppath + '/' + randhex)):
            os.mkdir(self._tmppath + '/' + randhex)

        versions.to_csv(self._tmppath + '/' +
                        randhex + "/versions.csv", index=False)
        empty = pd.DataFrame(
            columns=self.columns)
        empty.to_csv(self._tmppath + '/' +
                     randhex + "/deletedS3.csv", index=False)

        self.resource.meta.client.upload_file(
            self._tmppath + '/' + randhex + "/versions.csv", self.aws_bkt, self._s3subdirremote + "versions.csv")
        self.resource.meta.client.upload_file(
            self._tmppath + '/' + randhex + "/deletedS3.csv", self.aws_bkt, self._s3subdirremote + "deletedS3.csv")

    def _update_aws_creds(self, aws_creds_path):
        """Parse credentials from the clipboard and update them in our text file."""
        new_creds = pyperclip.paste()
        lines = []
        for line in new_creds.split('\n'):
            lines.append(line.strip())

        if len(lines) == 4 and lines[0][0] == '[' and lines[0][-1] == ']':
            role = lines[0][1:-1]

            existing_creds = []
            with open(aws_creds_path, 'r') as creds_file:
                for line in creds_file.readlines():
                    existing_creds.append(line.strip())

            found_role = False
            for i in range(len(existing_creds)):
                if existing_creds[i][1:-1] == role:
                    found_role = True
                    for j in range(len(lines)):
                        existing_creds[i+j] = lines[j]

            # If no role was found add it to the bottom
            if not found_role:
                for line in lines:
                    existing_creds.append(line)

            with open(aws_creds_path, 'w') as creds_file:
                for line in existing_creds:
                    creds_file.write(line + '\n')

            return True
        else:
            return False

    def _download_entire_prefix(self, bucket, prefix, local_path):
        """Download the entire contents of an S3 prefix."""
        response = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        try:
            response["Contents"]
        except:
            return

        s3_files = []
        for file_dict in response["Contents"]:
            s3_files.append(file_dict["Key"])

        local_paths = []
        for root, dirs, files in os.walk(local_path):
            for file in files:
                local_paths.append(os.path.join(root, file)[
                    len(local_path)+1:].replace('\\', '/'))

        for i in range(len(s3_files)):
            if(s3_files[i].startswith(prefix)):
                s3_files[i] = s3_files[i][len(prefix + '/')-1:]  # Remove?

        s3_to_download = {}
        for rel_file in s3_files:
            if rel_file not in local_paths:
                if len(os.path.basename(rel_file)) > 0:
                    target_location = os.path.join(prefix, rel_file)
                    s3_to_download[target_location] = os.path.join(
                        local_path, rel_file)

        if len(s3_to_download) > 0:
            for s3_file, local_path in s3_to_download.items():
                self._download_file(bucket, s3_file, local_path)

    def _download_file(self, bucket, s3_file, local_path):
        """Downloads a singe file from S3 and creates necessary dirs."""
        dirs = local_path.replace('\\', '/').split('/')[:-1]
        file_directories = ""
        for dir in dirs:
            file_directories += dir + '/'
            if(not os.path.exists(file_directories)):
                os.mkdir(file_directories)
        self.resource.meta.client.download_file(bucket, s3_file, local_path)

    def _compute_directory(self, directory, ignoreS3=True):
        """Create a dataframe describing all files in a local directory."""
        df = pd.DataFrame(columns=self.columns)
        files_under_folder = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                files_under_folder.append(os.path.join(root, file)[
                    len(directory)+1:].replace('\\', '/'))

        for file_path in files_under_folder:
            if self._s3id in file_path and ignoreS3:
                continue
            timestamp = dt.datetime.fromtimestamp(pathlib.Path(
                directory + '\\' + file_path).stat().st_mtime).strftime(self.dttm_format)

            new_df = pd.DataFrame(columns=self.columns)
            new_df[self._file_colname] = [file_path]
            new_df[self._time_colname] = timestamp
            new_df[self._hash_colname] = self._hash(
                directory + '\\' + file_path)
            new_df[self._editor_colname] = self._name
            df = pd.concat([df, new_df], ignore_index=True)
        return df

    def _compute_dfs(self, folder_path):
        """Return a list of dfs containing all the information for smart_sync."""
        mine = self._compute_directory(folder_path)
        other = pd.read_csv(self._s3versionspath)
        mine, other = self._filter_ignore(mine, other)
        inboth = mine[mine[self._file_colname].isin(other[self._file_colname])]

        mod_mine = []  # Files more recently modified Locally
        mod_other = []  # Files more recently modified on S3
        for file in inboth[self._file_colname]:
            mycs = inboth.loc[inboth[self._file_colname]
                              == file][self._hash_colname].iloc[0]
            othercs = other.loc[other[self._file_colname]
                                == file][self._hash_colname].iloc[0]
            if(mycs != othercs):
                datemine = dt.datetime.strptime(
                    mine.loc[mine[self._file_colname] == file][self._time_colname].iloc[0], self.dttm_format)
                dateother = dt.datetime.strptime(
                    other.loc[other[self._file_colname] == file][self._time_colname].iloc[0], self.dttm_format)
                if(datemine > dateother):
                    mod_mine.append([file, datemine, dateother])
                else:
                    mod_other.append([file, datemine, dateother])
        return (mine, other, mod_mine, mod_other)

    def _filter_ignore(self, mine, other):
        """Remove all files that should be ignored as requested by the user."""
        mine = mine.loc[~mine[self._file_colname].isin(self._ignore)]
        other = other.loc[~other[self._file_colname].isin(self._ignore)]
        return mine, other

    def _upload_to_s3(self, files):
        """Attempt to upload every file provided to the remote S3 prefix."""
        successful = []
        for file in files:
            print("Uploading " + file + " to S3...")
            try:
                self.resource.meta.client.upload_file(
                    self.datafolder + '/' + file, self.aws_bkt, self.aws_prfx + '/' + file)
                successful.append(file)
            except Exception as exc:
                print("ERROR: Couldn't upload." + file)
                print(
                    "Please check the error log for more information. Skipping this file.")
                self._log += "-----------------------------upload_to_s3 error-----------------------------\n"
                self._log += "File: " + self.datafolder + '/' + file + '\n'
                self._log += "S3 Bucket: " + self.aws_bkt + '\n'
                self._log += "S3 Key: " + self.aws_prfx + '/' + file + "\n\n"
                self._log += "Line number: " + \
                    str(sys.exc_info()[2].tb_lineno) + '\n'
                self._log += "Message: " + str(exc) + "\n\n\n"
        return successful

    def _download_from_s3(self, files):
        """Attempt to download every file provided from the remote S3 prefix."""
        for file in files:
            print("Downloading " + file + " from S3...")
            try:
                self._download_file(self.aws_bkt, self.aws_prfx + '/' +
                                    file, self.datafolder + '/' + file)
            except Exception as exc:
                print("ERROR: Couldn't download " + file)
                print(
                    "Please check the error log for more information. Skipping this file.")
                self._log += "-----------------------------download_from_s3 error-----------------------------\n"
                self._log += "File: " + self.datafolder + '/' + file + '\n'
                self._log += "S3 Bucket: " + self.aws_bkt + '\n'
                self._log += "S3 Key: " + self.aws_prfx + '/' + file + "\n\n"
                self._log += "Line number: " + \
                    str(sys.exc_info()[2].tb_lineno) + '\n'
                self._log += "Message: " + str(exc) + "\n\n\n"

    def _delete_from_s3(self, files):
        """Attempt to delete every file provided from the remote S3 prefix."""
        print("Are you sure you want to delete these files from S3?")
        inp = input(", ".join(files) + "\tY/N: ")
        successful = []
        if(inp.lower() in ['y', 'yes']):
            for file in files:
                print("Deleting " + file + " from S3...")
                localFile = file.split('/')[-1]
                downloaded = False
                deleted = False
                reuploaded = False

                try:
                    # Download the file from S3 into our tmp folder, delete it from S3, and
                    # reupload from our tmp folder into the .S3/deleted folder on AWS
                    self._download_file(self.aws_bkt, self.aws_prfx + '/' + file,
                                        self._tmppath + '/' + localFile)
                    downloaded = True
                    self.client.delete_object(
                        Bucket=self.aws_bkt, Key=self.aws_prfx + '/' + file)
                    deleted = True
                    self.resource.meta.client.upload_file(
                        self._tmppath + '/' + localFile, self.aws_bkt, self._s3subdirremote + 'deleted/' + localFile)
                    reuploaded = True
                    successful.append(file)
                except Exception as exc:
                    print("ERROR: Couldn't delete " + file + " from S3.")
                    print(
                        "Please check the error log for more information. Skipping this file.")
                    self._log += "-----------------------------delete_from_s3 error-----------------------------\n"
                    self._log += "S3 Bucket: " + self.aws_bkt + '\n'
                    self._log += "S3 Key: " + self.aws_prfx + '/' + file + '\n'
                    self._log += "File Downloaded To: " + self._tmppath + '/' + localFile + '\n'
                    self._log += "S3 Key Reupload: " + \
                        self._s3subdirremote + "deleted/" + localFile + '\n'
                    self._log += "Downloaded: " + str(downloaded) + '\n'
                    self._log += "Deleted: " + str(deleted) + '\n'
                    self._log += "Reuploaded: " + str(reuploaded) + "\n\n"
                    self._log += "Line number: " + \
                        str(sys.exc_info()[2].tb_lineno) + '\n'
                    self._log += "Message: " + str(exc) + "\n\n\n"
        return successful

    def _delete_from_local(self, files):
        """Attempt to delete every file requested from our local data folder."""
        print("Are you sure you want to delete these files from your computer?")
        inp = input(", ".join(files) + '\tY/N: ')
        if(inp.lower() in ['y', 'yes']):
            for file in files:
                try:
                    os.remove(self.datafolder + '/' + file)
                except Exception as exc:
                    print("ERROR: Couldn't delete " +
                          file + " from your computer.")
                    print(
                        "Please check the error log for more information. Skipping this file.")
                    self._log += "-----------------------------delete_from_local error-----------------------------\n"
                    self._log += "File: " + self.datafolder + '/' + file + "\n\n"
                    self._log += "Line number: " + \
                        str(sys.exc_info()[2].tb_lineno) + '\n'
                    self._log += "Message: " + str(exc) + "\n\n\n"

    def _apply_selected_indices(self, data_function, allfiles):
        """Prompt the user to select certain files to perform a synchronization function on."""
        indicies = input(
            "\n(enter numbers separated by commas, enter for cancel, 'all' for all):")
        selectedfiles = []
        if indicies.lower() in ['a', "all"]:
            selectedfiles = data_function(allfiles)
        elif indicies != "":
            indicies = set(indicies.split(','))
            for num in indicies:
                num = int(num)
                if num >= 0 and num < len(allfiles):
                    selectedfiles.append(allfiles[num])
            selectedfiles = data_function(selectedfiles)
        return selectedfiles

    def _push_sequence(self, listfiles, mine, other):
        """User-prompted uploading of files from a dataframe."""
        to_push = []
        index = 0
        for file in listfiles:
            to_push.append(file[0])
            print(index, file[0], '\t', file[1], '\t', file[2], "\t by",
                  other.loc[other[self._file_colname] == file[0]][self._editor_colname].iloc[0])
            index += 1
        selectedpush = self._apply_selected_indices(
            self._upload_to_s3, to_push)

        updatedins3 = mine.loc[mine[self._file_colname].isin(
            selectedpush)]
        newversions = pd.concat([other, updatedins3])
        newversions = newversions.drop_duplicates(
            [self._file_colname], keep="last").sort_index()
        newversions.to_csv(self._s3versionspath, index=False)
        print("Done.\n")

    def _push_modified_s3(self):
        """Update files on S3 with modifications that were made locally more recently."""
        mine, other, mod_mine, mod_other = self._compute_dfs(self.datafolder)
        if(len(mod_mine) > 0):
            print(
                "UPLOAD: Would you like to update these files on S3 with your local changes?:")
            print(
                "('file name' / 'Date last modified locally' / 'Date last modified on S3')\n")
            self._push_sequence(mod_mine, mine, other)

    def _revert_modified_s3(self):
        """Revert files on S3 with modifications that were made locally less recently."""
        mine, other, mod_mine, mod_other = self._compute_dfs(self.datafolder)
        if(len(mod_other) > 0):
            print(
                "UPLOAD: Would you like to revert these files on S3 back to your local versions?:")
            print(
                "('file name' / 'Date last modified locally' / 'Date last modified on S3')\n")
            self._push_sequence(mod_other, mine, other)

    def _pull_sequence(self, listfiles, other):
        """User-prompted downloading of files from a dataframe."""
        to_pull = []
        index = 0
        for file in listfiles:
            to_pull.append(file[0])
            print(index, file[0], '\t', file[1], '\t', file[2], "\t by",
                  other.loc[other[self._file_colname] == file[0]][self._editor_colname].iloc[0])
            index += 1
        self._apply_selected_indices(self._download_from_s3, to_pull)
        print("Done.\n")

    def _pull_modified_local(self):
        """Update local files with modifications that were made on S3 more recently."""
        mine, other, mod_mine, mod_other = self._compute_dfs(self.datafolder)
        if(len(mod_other) > 0):
            print(
                "DOWNLOAD: Would you like to update these local files with the changes from S3?:")
            print(
                "('file name' / 'Date last modified locally' / 'Date last modified on S3')\n")
            self._pull_sequence(mod_other, other)

    def _revert_modified_local(self):
        """Revert local files with modifications that were made on S3 less recently."""
        mine, other, mod_mine, mod_other = self._compute_dfs(self.datafolder)
        if(len(mod_mine) > 0):
            print(
                "DOWNLOAD: Would you like to revert these local files back to the versions on S3?:")
            print(
                "('file name' / 'Date last modified locally' / 'Date last modified on S3')\n")
            self._pull_sequence(mod_mine, other)

    def _push_new_s3(self):
        """Upload files to S3 that were created locally."""
        mine, other, mod_mine, mod_other = self._compute_dfs(self.datafolder)

        # Find files that are in our directory but not AWS, and load in files deleted from AWS
        new_local = mine.loc[~mine[self._file_colname].isin(
            other[self._file_colname])]
        deletedfiles = pd.read_csv(self._s3delpath)[
            self._file_colname].values.tolist()

        if(len(new_local) > 0):
            print(
                "UPLOAD: Would you like to upload these new files to S3 that were created locally?:")
            print("('file name' / 'Date last modified Locally')\n")
            to_add = []
            index = 0
            for i, row in new_local.iterrows():
                to_add.append(row[self._file_colname])
                print(index, row[self._file_colname],
                      '\t', row[self._time_colname], end='\t')
                if(row[self._file_colname] in deletedfiles):
                    print("*DELETED ON S3", end='')
                print()
                index += 1

            selectedadd = self._apply_selected_indices(
                self._upload_to_s3, to_add)

            added_to_s3 = mine.loc[mine[self._file_colname].isin(selectedadd)]
            newversions = pd.concat([other, added_to_s3])
            newversions.to_csv(self._s3versionspath, index=False)
            print("Done.\n")

    def _push_deleted_s3(self):
        """Remove files from S3 that were deleted locally."""
        mine, other, mod_mine, mod_other = self._compute_dfs(self.datafolder)

        # Load in what files we had last time, and what files we have deleted in the past
        oldmine = pd.read_csv(self._localversionspath)
        deletedlocal = pd.read_csv(self._localdelpath)
        # Combine a list of files we have deleted and files we have had in the past, remove any duplicates
        deletedlocal = pd.concat([oldmine, deletedlocal])
        deletedlocal = deletedlocal.drop_duplicates(
            [self._file_colname], keep="last")
        # From previous files + deleted files select only the ones that AREN'T in our local system but ARE on AWS
        deletedlocal = deletedlocal[~deletedlocal[self._file_colname].isin(
            mine[self._file_colname])]
        deletedlocal = other[other[self._file_colname].isin(
            deletedlocal[self._file_colname])]

        deletedlocal.to_csv(self._localdelpath, index=False)

        if(len(deletedlocal) > 0):
            print(
                "UPLOAD: Would you like to delete these files on S3 that were deleted locally?:")
            print("('file name' / 'Date last modified on S3')\n")
            to_delete = []
            index = 0
            for i, row in deletedlocal.iterrows():
                to_delete.append(row[self._file_colname])
                print(index, row[self._file_colname], '\t',
                      row[self._time_colname], '\t by', row[self._editor_colname])
                index += 1

            selecteddelete = self._apply_selected_indices(
                self._delete_from_s3, to_delete)

            deleteds3 = pd.read_csv(self._s3delpath)
            newdeleted = other.loc[other[self._file_colname].isin(
                selecteddelete)]
            deleteds3 = pd.concat([deleteds3, newdeleted])
            deleteds3.to_csv(self._s3delpath)

            # Replace any removed file names with N/A and then drop if they have been deleted
            other[self._file_colname] = other[self._file_colname].where(
                ~other[self._file_colname].isin(selecteddelete))
            other = other.dropna()
            other.to_csv(self._s3versionspath, index=False)
            print("Done.\n")

    def _pull_new_local(self):
        """Download files from S3 that were created recently."""
        mine, other, mod_mine, mod_other = self._compute_dfs(self.datafolder)

        # Find files that are on S3 but not our local system and read in files we have deleted locally
        news3 = other.loc[~other[self._file_colname].isin(
            mine[self._file_colname])]
        deletedfiles = pd.read_csv(self._localdelpath)[
            self._file_colname].values.tolist()

        if(len(news3) > 0):
            print(
                "DOWNLOAD: Would you like to download these new files that were created on S3?:")
            print("('file name' / 'Date last modified on S3')\n")
            to_download = []
            index = 0
            for i, row in news3.iterrows():
                to_download.append(row[self._file_colname])
                print(index, row[self._file_colname], '\t', row[self._time_colname],
                      "\t by", row[self._editor_colname], end='\t')
                if(row[self._file_colname] in deletedfiles):
                    print("*DELETED LOCALLY", end='')
                print()
                index += 1

            self._apply_selected_indices(self._download_from_s3, to_download)
            print("Done.\n")

    def _pull_deleted_local(self):
        """Remove files from local system that were deleted on S3."""
        mine, other, mod_mine, mod_other = self._compute_dfs(self.datafolder)

        # Load in files deleted from S3, and select only those that ARE on our local system and AREN'T on AWS
        deleteds3 = pd.read_csv(self._s3delpath)
        deleteds3 = deleteds3[deleteds3[self._file_colname].isin(
            mine[self._file_colname])]
        deleteds3 = deleteds3[~deleteds3[self._file_colname].isin(
            other[self._file_colname])]

        if(len(deleteds3) > 0):
            print(
                "DOWNLOAD: Would you like to delete these files from your computer that were deleted on S3?:")
            print("('file name' / 'Date last modified locally')\n")
            to_delete = []
            index = 0
            for i, row in deleteds3.iterrows():
                to_delete.append(row[self._file_colname])
                mask = row[self._file_colname] == mine[self._file_colname]
                print(index, row[self._file_colname], '\t',
                      mine.loc[mask][self._file_colname].iloc[0])
                index += 1

            self._apply_selected_indices(self._delete_from_local, to_delete)
            print('Done.\n')
