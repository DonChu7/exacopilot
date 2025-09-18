#!/usr/bin/env python
#
# dcli.py1
#
# Copyright (c) 2008, 2025, Oracle and/or its affiliates.
#
# OK! pylint: disable=deprecated-module
# OK! pylint: disable=protected-access
#
#    NAME
#      dcli.py - distributed shell for Oracle storage
#
#    DESCRIPTION
#      distributed shell for Oracle storage
#
#    NOTES
#       requires Python version 2.5 or greater
# --------------------------
# Typical usage:
#
# create a text file of hosts named mycells
#
# execute a shell command on all cells:
#    dcli -g mycells "ls -l "
#
# or excute a cellcli command using -c option to specify cells:
#    dcli  -c sgbs21,sgbs22 cellcli -e list cell detail
#
# or do test printing of cell names:
#    dcli -g mycells -t
#
# or create a file to be copied and executed to a group of cells:
#    dcli  -g mycells -x cellwork.pl
#    dcli  -g mycells -x cellclicommands.scl
#
# File extension ".scl" is interpreted as a cellcli script file.
# When -x option value is a ".scl" file, then the file is copied
# and is used as input to cellcli on target cells.
#
# This program uses SSH for security between
# the host running dcli and the target cells.
#
#    MODIFIED   (MM/DD/YY)
#    vtholath    06/13/25 - bug 38021461: print 'hosts' instead of 'cells'
#    shenyliu    05/01/25 - bug 37793848: support password with special
#                           characters with --key-with-one-password option.
#    shenyliu    04/09/25 - bug 37694117: show Permission Denied immediately
#                           when password is wrong.
#    shenyliu    03/17/25 - bug 37705638: create directory with -f option if
#                           it does not exist
#    shenyliu    02/06/25 - bug 37356088: add batchsize for all commands based
#                           on fd limit
#    shenyliu    01/27/25 - bug 37355358: auto-create ssh key if unavailable;
#                           auto-retry for --key-with-one-password if offending
#                           key is present
#    shenyliu    12/11/24 - an enhancement for timeout option when connecting
#                           to a hanging host.
#    shenyliu    10/14/24 - bug 37163937: add auto-retry for re-imaged node and
#                           decoding standards
#    shenyliu    10/09/24 - bug 37016465: remove testCells, and add option
#                           --ctimeout
#    anpereir    09/18/24 - Bug 37039888: UnicodeEncodeError with Python2.x
#    shenyliu    09/13/24 - bug 36282725: support large numbers of
#                           target cells
#    shenyliu    07/19/24 - enh 36696607: add an option to timeout
#                           remote commands
#    shenyliu    07/19/24 - enh 36696607: add an option to timeout
#                           remote commands
#                           remote commands
#    shenyliu    07/11/24 - Bug 36696613: add an option to keep the blank space
#                           in the front
#    saheranj    01/25/24 - change connect timeout from 1 to 2 seconds
#    rohansen    11/01/23 - support connect through jumphost. bug 35828305
#    rohansen    01/05/23 - fix regress bug 34902079
#    rohansen    12/01/22 - support python 3.10.2. bug 34357152
#    rohansen    10/28/22 - fix grep on auth_keys file. Bug 34734974
#    rohansen    03/10/22 - prefer RSA keys for key setup. bug 33911028
#    rohansen    09/09/21 - support special char for onepw option. bug 33333867
#    rohansen    04/05/21 - fix to run with python 3 or 2
#    rohansen    09/02/20 - add lroot-exatmp to avoid aide alerts. bug 29213377
#    rohansen    06/18/20 - Add one pw option for key setup. bug 31170489
#    muraghav    08/19/19 - 29913678 - ADD SERVERALIVEINTERVAL AND
#                           SERVERALIVECOUNTMAX TO DCLI
#    rohansen    11/05/18 - retry connect during extended timeout. bug 28885873
#    rohansen    09/10/18 - add connect timeout option. bug 28613512
#    rohansen    12/04/17 - fix unkey. bug 27201472
#    rohansen    03/02/16 - fix execution order for serial option. bug 22713759
#    rohansen    07/29/15 - support raw ipv6 file copy. bug 21518948
#    rohansen    07/21/15 - support ipv6. bug 21481514
#    chienguy    05/06/14 - Bug 18502556 - Corrected --unkey option.
#    rohansen    08/28/13 - support bourne shell. bug 13725681
#    ihonda      05/09/13 - bug 16705313: suppress "Broken pipe" error
#    rakkayaj    05/08/13 - Capture ssh error when no remote command specified
#                           (login)
#    mingmche    09/11/12 - bug 14187446: add batchsize option for dcli
#    rdani       08/17/12 - rollback rakkayaj_bug-13822165 for now. Causes
#                           patchmgr hang
#    rakkayaj    05/15/12 - bug 13822165: supress motd
#    mpopeang    10/25/11 - bug11725440: allow multiple file copy
#    chienguy    06/17/11 - Bug 11874358 - Updated dcli to output in chunks for
#                           a single cell, or when in serialized mode. When not
#                           in serialized mode and with multiple cells,
#                           truncate the output at maxLines.
#    rohansen    04/04/11 - fix error message to use rsa
#    rohansen    01/05/11 - fix grep option portablility bug 10629030
#    rohansen    08/04/10 - support python 2.6 deprecated popen
#    rohansen    03/02/10 - support key removal option
#    rohansen    12/23/08 - support directory copy and destination option
#    rohansen    10/29/08 - added quotes to prevent shell expansion
#    rohansen    09/16/08 - add vmstat option
#    rohansen    09/11/08 - kill child processes after ctrl-c
#    rohansen    06/27/08 - add -k option to push keys to cells
#    sidatta     07/22/08 - Changing name to dcli
#    rohansen    04/29/08 - more options
#    rohansen    04/01/08 - Creation
#
# --------------------------

"""
Distributed Shell for Oracle Storage

This script executes commands on multiple cells in parallel threads.
The cells are referenced by their domain name or ip address.
Local files can be copied to cells and executed on cells.
This tool does not support interactive sessions with host applications.
Use of this tool assumes ssh is running on local host and cells.
The --key-with-one-password option should be used initially to perform key
exchange with cells. User may be prompted for the remote user password.
When using the -k option, the keying step is serialized to prevent overlayed
prompts.
when using the --key-with-one-password option, the keying step is done in
parallel.
After the key exchange is done once, the subsequent commands to the same cells
do not require -k or --key-with-one-password options. Also, password is not
required anymore.
Command output (stdout and stderr) is collected and displayed after the
copy and command execution has finished on all cells.
Options allow this command output to be abbreviated.
Python version 2.5 is required (to support absolute_import, required by pylint)

Return values:
 0 -- file or command was copied and executed successfully on all cells
 1 -- one or more cells could not be reached or remote execution
      returned non-zero status.
 2 -- An error prevented any command execution

Examples:
 dcli -g mycells -k
 dcli -c stsd2s2,stsd2s3 vmstat
 dcli -g mycells cellcli -e alter iormplan active
 dcli -g mycells -x reConfig.scl
"""
from __future__ import absolute_import
from __future__ import division
import os
import os.path
import time
import stat
import re
import sys
import socket
import threading
import signal
import glob
import tempfile
import getpass
from optparse import OptionParser
import io
import subprocess
import base64

# dcli version displayed with --version
version = "3.5"
# uniform password for pushing keys
# if None then we we prompt for pw
ONEPW = ""
# default assignment for SSH port
PORT = 22
# Bug 37016465: seconds to wait for a connection to be established before
# giving up
DEFAULT_CONNECTION_TIMEOUT = 5
# default user is celladmin
DEFAULT_USER = "celladmin"
EXA_TMP_DIR = "/var/log/exadatatmp/"
# default location of SSH program
SSH = "/usr/bin/ssh"
# default location of SCP program
SCP = "/usr/bin/scp"
# SSH file definitions:
SSHSUBDIR=".ssh"
SSHDSAFILE="id_dsa.pub"
SSHRSAFILE="id_rsa.pub"
SSHKEY=[]
# 29913678 - ADD SERVERALIVEINTERVAL AND SERVERALIVECOUNTMAX TO DCLI.
# Add default ssh options
# This was added since we have server timeouts running sundiag on hosts
# leading to not being able to collect the required information on Exadata
# nodes. Hence setting these default values.
SSH_OPTION_SERVERALIVEINTERVAL="-o ServerAliveInterval=60"
SSH_OPTION_SERVERALIVECOUNTMAX="-o ServerAliveCountMax=5"
# Bug 37016465: ConnectTimeout option specifies the amount of time (in seconds)
# that the SSH client will wait for a connection to be established before
# giving up. User is able to customize by option -ctimeout.
SSH_OPTION_CONNECTTIMEOUT="-o ConnectTimeout="

# Error class used to handle environment errors (e.g. file not found)
class Error(Exception):
    def __init__(self, msg):
        super(Error,self).__init__(msg)
        self.msg = msg

# UsageError class is used to handle errors caused by invalid options
class UsageError(Exception):
    def __init__(self, msg):
        super(UsageError,self).__init__(msg)
        self.msg = msg

def buildCellList(cells, filename):
    """
    Build a list of unique cells which will be contacted by dcli.

    Takes a list of cells and a filename.
    The file is read, and each non-empty line that does not start with #
    is assumed to be a cell.
    Unique cells are added to a list.
    Returns the list of unique cells.
    """
    celllist = []
    if filename :
        filename = filename.strip()
        try :
            fd = open(filename)
            lines = fd.readlines()
            for line in lines :
                line = line.strip()
                if len(line) > 0 and not line.startswith("#") :
                    celllist.append(line)
        except IOError as err:
            raise Error("I/O error(%s) on %s: %s" %
                        (err.errno, filename, err.strerror))

    if cells :
        for cline in cells:
            cellSplit = cline.split(",")
            for cell in cellSplit :
                celllist.append(cell.strip())

    uniqueCellList = []
    for c in celllist :
        if c not in uniqueCellList:
            uniqueCellList.append(c)
    return uniqueCellList


def buildCommand( args, options ):
    """
    Build a command string to be sent to all hosts.

    Input options.hideStderr when true, suppresses the stderr of
          remotely executed commands. Default is false.
    Input options.rootWithExaTmp when true, uses exadatatmp dir
    Command arguments can be used to build the command to
    be sent to hosts.
    Input options.timeout when non-null, sets up a timeout in
    seconds for the whole command. It will stop all ongoing work;
    previous output will be retained.
    """
    command = "("
    if options.rootWithExaTmp:
        command += "cd "+ EXA_TMP_DIR+ ";"
    if args:
        for word in args:
            command += " " + word
    if options.hideStderr:
       command += ") 2>/dev/null"
    else:
       command += ") 2>&1"
    if options.timeout:
       # we assume all nodes support POSIX system (Including Linux).
       command = "timeout %ss /bin/sh -c '(%s)'" % (options.timeout, command)
    return command

def findFiles(path):
    '''Return list of files matching pattern in path.'''

    file_list = []
    path = os.path.expanduser(path)
    path = os.path.expandvars(path)
    file_list = glob.glob(path)

    return file_list

def checkFile( filepath, isExec ):
    """
    Test for existence and permissions of files to be copied or executed remotely.

    The file is tested for read and execute permissions.
    """
    files = findFiles(filepath)

    if not files:
       raise Error("File does not exist: %s" % filepath )
    else:
       for a_file in files:
          if not os.path.exists(a_file):
             raise Error("File does not exist: %s" % a_file )
          if isExec:
             if not os.path.isfile(a_file):
                raise Error("Exec file is not a regular file: %s" % a_file )
          elif not os.path.isfile(a_file) and not os.path.isdir(a_file):
              raise Error("File is not a regular file or directory: %s" %
                          a_file )
          st = os.stat(a_file)
          mode = st[stat.ST_MODE]
          if isExec and os.name == "posix" and not (mode & stat.S_IEXEC):   # same as stat.S_IXUSR
             raise Error("Exec file does not have owner execute permissions")

def checkKeys(prompt, verbose):
    """
    Test for existence of rsa public keys for current user.

    Search for rsa public key files in the current users
    .ssh directory.  The key file is read and will be sent to
    the remote cells to be added to authorized_key file.
    The default public key file names for ssh protocol version 2 are
    sought. This is id_rsa.pub in ~/.ssh.
    """
    sshDir = os.path.join( os.path.expanduser("~"), SSHSUBDIR )
    rsaKeyFile = os.path.join( sshDir, SSHRSAFILE )
    dsaKeyFile = os.path.join( sshDir, SSHDSAFILE )
    if Session.testmode:
        SSHKEY.append("ThisIsYourKey")
    elif os.path.isfile(rsaKeyFile):
        f = open(rsaKeyFile )
        SSHKEY.append( f.read().strip() )
        if (verbose ): print("RSA KEY: " + SSHKEY[-1])
        f.close()
    elif os.path.isfile(dsaKeyFile):
        f = open(dsaKeyFile )
        # we display DSA key for debugging, but we don't use it
        # because it's not supported starting with SSH version 7.0
        if (verbose ): print("DSA KEY: " + SSHKEY[-1])
        f.close()
    if not SSHKEY:
        # No RSA key found, attempt to generate one
        try:
            if not os.path.exists(sshDir):
                os.makedirs(sshDir)
            if prompt:
                subprocess.call(["ssh-keygen", "-t", "rsa"])
            else:
                rsaPrivateKey = os.path.join(sshDir, "id_rsa")
                subprocess.call(["ssh-keygen", "-t", "rsa", "-f", rsaPrivateKey,
                                "-N", ""])
            if verbose:
                print("An RSA key pair was generated.")
            # Load the newly generated key
            with open(rsaKeyFile) as f:
                SSHKEY.append(f.read().strip())
        except Exception as e:
            raise Error(
                "Failed to auto-generate RSA key pair for the current "
                "user: {0}. Run 'ssh-keygen -t rsa' to generate an SSH key"
                " pair.\n".format(e)
            )

# Session class holds attributes for this invocation of dcli
class Session:
    def __init__(self):
        pass
    onepw = ""
    testmode = ""
    exc = None

def  getOnePw():
    """
    Prompt for one password which will be used to push keys to all cells.

    Session.onepw is a uniform password for pushing keys
    if None then we we prompt for pw
    at the top of dcli.py.
    We escape curly brackets because they are used by expect and this
    allows special characters to be in passwords, e.g. $ ; # !
    this routine is called if the "--key-with-one-password" option is set
    """

    if (Session.onepw):
        return

    onepw = getpass.getpass("Password: ")
    Session.onepw = onepw

def checkVmstat( vmstatOptions ):
    """
    Check vmstat option for valid periodic statistic options.

    Returns a repeat count and a command to be sent to cells.
    Returns null for repeat count if the option appears to be not periodic,
    e.g. -f, -s, -m, -p, -d, -V
    Periodic options, delay, and count are transformed into repeat count
    and modified command.
    Periodic options are "-n, -a, -S"
    Repeat count returned is either 1 or the last number in the option.
    Count of -1 indicates no repeat was given, so repeat indefinitely.
    Modified command is also returned, which is the command sent to cells.
    The repeat count will be appended in command loop
    --vmstat=       count       command
    ""              1           "vmstat"
    "3"              -1         "vmstat 3 "
    "3 10"           10         "vmstat 3 "
    "2 1"           1           "vmstat 2 "
    "-a 3"          -1          "vmstat -a 3 "
    """
    repeat = None
    delay = None
    vmstatCommand = "vmstat "
    vmOpts = vmstatOptions.split()
    for op in vmOpts:
        if op in ("-f","-s","-m","-p","-D", "-d","-V"):
            return None, None

        num = getInt(op)
        # less that 1 for delay or count is invalid
        if num != None and num < 1 :
            return None, None
        if num:
            if repeat :
                # more than 2 numbers as options
                return None, None
            elif delay:
                repeat = num
            else:
                delay = num
        elif op != "-n":
            # we handle -n ourselves
            vmstatCommand += op + " "
    #default delay is immediate (no repeat)
    if  delay:
        vmstatCommand += "%d " % delay

        # default repeat is infinite
        if not repeat:
            repeat = -1

    else:
        #without delay, default repeat is 1
        vmstatCommand += "1 "
        repeat = 1

    return repeat, vmstatCommand


def copyAndExecute( cells, copyfiles, exec_file, destfile, command, options ) :
    """
    Send files or a command to execute on a list a cells.

    A thread is started for each cell.
    Input cells is a list of cells.
    Input command is string to be executed via ssh on each cell.
    Input copyfiles is a list of files to be copied to each cell over scp.
    Input exec_file is a file to be copied and executed on each cell.
    Input user is login name to be used on remote cells
    Input rootWithExaTmp is true to use exadatatmp default dir
    Input pushKey is true if key is to be pushed to remote cells
    Input dropKey is true if key is to be removed from remote cells
    Input maxLines is max lines in a chunk of output
    Input options is ssh or scp options to be passed through to ssh or scp
    Input scpOptions are scp options to be passed through to scp
    Input serialize is true if operations should be serialized
    Input verbose is true for extra output
    The response is collected as a list of lines.
    Finally wait for all cells to complete and
    Return status map (return codes per cell).
    output map (lines from stdout and stderr per cell) and
    a list of cells which are unable to connect.
    """
    if options.userID:
        user = options.userID
    else:
        if options.rootWithExaTmp:
            user = "root"
        else:
            user = DEFAULT_USER
    pushKey = options.pushKey or options.konepw
    konepw = options.konepw
    dropKey = options.dropKey
    hideStderr = options.hideStderr
    rootWithExaTmp = options.rootWithExaTmp
    maxLines = options.maxLines
    sshOptions = options.sshOptions
    # prompt for password if --prompt or pushing keys
    batchmode = not ( options.prompt or pushKey )
    # prompt for host key checking if --prompt, -k or --key-with-one-password
    strictHostKeyChecking = not ( options.prompt or pushKey)
    showBanner = options.showBanner
    scpOptions = options.scpOptions
    serialize = options.serializeOps or options.pushKey
    verbose = options.verbosity
    connectTimeout = options.ctimeout

    files = list()
    updateLock = threading.Lock()
    badCells = []
    cells_need_retry = []

    class WorkThread (threading.Thread):
        """
        Command thread issues one command to one cell.

        one thread is created for each cell
        allowing parallel operations.
        """
        def __init__( self, cell ):
             threading.Thread.__init__(self)
             self.cell = cell
             self.child = None
             self.output_truncated = 0
        def run(self):
            """
            One thread for each WorkThread.start()
            """
            if verbose : print("...entering thread for %s:" % self.cell)
            childStatus = 0
            childInput = []
            childOutput = []
            opString = " "
            scpOpString = " "
            if sshOptions:
                opString += sshOptions + " "
            if batchmode:
                opString += "-o BatchMode=yes "
            if strictHostKeyChecking:
                opString += "-o strictHostKeyChecking=yes "
            if connectTimeout:
                opString += SSH_OPTION_CONNECTTIMEOUT +\
                            str(connectTimeout) + " "
            else:
                opString += SSH_OPTION_CONNECTTIMEOUT +\
                            str(DEFAULT_CONNECTION_TIMEOUT) + " "
            # Add default ServerAliveInterval if not passed
            if not sshOptions or \
              sshOptions.lower().find("serveraliveinterval=") < 0 :
                opString += SSH_OPTION_SERVERALIVEINTERVAL + " "
            # Add default ServerAliveMaxCount if not passed
            if not sshOptions or \
              sshOptions.lower().find("serveralivecountmax=") < 0 :
                opString += SSH_OPTION_SERVERALIVECOUNTMAX + " "
            if scpOptions:
                scpOpString += scpOptions + " "
            else:
                scpOpString = opString
            if exec_file and scpOpString.find("-p") < 0 :
                scpOpString += "-p "

            sshUser = ""
            scpHost = self.cell
            if files:
                try:
                    # check for ipv6 address, scp requires backets
                    socket.inet_pton(socket.AF_INET6, scpHost)
                    scpHost = "[" + scpHost + "]"
                except socket.error:
                    # not a v6 address
                    pass
            if user:
                sshUser = "-l " + user + " "
                scpHost = user + "@" + scpHost

            if SSHKEY and pushKey:
                # Perform the -k or --key-with-one-password option step by
                # sending the public key to cell
                # This will be serialized because host identity and password prompts
                # could overlay each other if the occur together.
                keys = SSHKEY[0]
                if len(SSHKEY)> 1:
                    keys += "\\|" + SSHKEY[1]
                sshCommand = "ssh " + opString + sshUser + self.cell +  \
                    " \" cd; mkdir -pm 700 .ssh; if grep '^\\s*" + keys + \
                    "' .ssh/authorized_keys  > /dev/null 2>&1 ; then echo ssh key already exists ; elif echo '" + \
                    SSHKEY[0] + "' >> .ssh/authorized_keys ; then chmod 644 .ssh/authorized_keys ;" + \
                    " echo ssh key added ; fi \""
                if konepw:
                    # bug 37793848: handle the password with special chars.
                    # Base64 encodes the password
                    # for example, it will convert password '[]{}Aa1@#$%'
                    # to 'W117fUFhMUAjJCU='
                    if sys.version_info[0] < 3:
                        # Python 2: password type is bytes
                        # python 2 base64 encoding takes bytes as input and
                        # returns string.
                        encoded_pw = base64.b64encode(Session.onepw)
                    else:
                        # Python 3: password type is string
                        # Python 3 base64 encoding takes bytes as input, so we
                        # first convert it to bytes; 
                        # Python 3 base64 encoding returns bytes, so we
                        # decode the return to string in the end.
                        encoded_pw = base64.b64encode(
                                Session.onepw.encode('utf-8')).decode('utf-8')
                    childInput = ["spawn -noecho " + sshCommand,
                                  "expect {",
                                  "\"Permission denied*\" { exit 255 }",
                                  # Decode the password
                                  # for example, 'W117fUFhMUAjJCU=' will be
                                  # decoded to '[]{}Aa1@#$%'
                                  "*password: {",
                                  "set encoded_pw {" + encoded_pw + "}",
                                  "set decoded_pw [exec printf %s " +
                                  "$encoded_pw | base64 -d]",
                                  "send -- \"$decoded_pw\\n\"",
                                  "exp_continue }",
                                  "*yes/no*)? {send yes\\n",
                                  "exp_continue }",
                                  "}"
                    ]
                    sshCommand = "expect"

                if Session.testmode:
                    sshCommand = "echo " + sshCommand
                childStatus, l = self.runCommand( sshCommand, True, childInput)
                childOutput.extend(l)
            
            # bug-37705638: Create destination directory on remote host
            # If destfile ends with "/" (indicating a directory path), we
            # check if it exists on the remote host and create if needed.
            # This prevents SCP failures with the error "<destfile> is not
            # a directory" when attempting to copy files to a non-existent
            # directory.
            if not childStatus and destfile and destfile.endswith("/"):
                mkdir_command = " 'mkdir -p " + destfile + "'"
                if  Session.testmode:
                    # for testing
                    sshCommand = "echo ssh " + opString + sshUser + self.cell \
                    + " " + mkdir_command
                else:
                    sshCommand = SSH + opString + sshUser + self.cell + " " +\
                    mkdir_command
                childStatus, l = self.runCommand( sshCommand, serialize, None )
                childOutput.extend(l)

            if not childStatus and files :

                list_string = ""
                for item_file in files:
                    list_string += " " + item_file

                if  Session.testmode:
                    # for testing
                    scpCommand = "echo scp " + list_string +  " " + scpHost + ":" + destname
                else:
                    scpCommand = SCP + scpOpString + list_string +  " " + scpHost + ":" + destname

                childStatus, l = self.runCommand( scpCommand, serialize, None)
                childOutput.extend(l)

            if not childStatus and command :
                if  Session.testmode:
                    # for testing
                    sshCommand = "echo ssh " + opString + sshUser  + self.cell + " " + command
                else:
                    sshCommand = SSH + opString + sshUser + self.cell + " " + command

                childStatus, l = self.runCommand( sshCommand, serialize, None )
                childOutput.extend(l)

            if not childStatus and SSHKEY and dropKey:
                # Perform the -unkey option step by sending the public key to cell
                keys = SSHKEY[0]
                if len(SSHKEY)> 1:
                    keys += "\\|" + SSHKEY[1]
                sshCommand = "ssh " + opString + sshUser + self.cell +  \
                    " \" if ! grep '^\\s*" + keys + \
                    "' .ssh/authorized_keys > /dev/null 2>&1 ; then echo ssh key did not exist ; elif " + \
                    "cp -f .ssh/authorized_keys .ssh/authorized_keys__;sed -i''  --follow-symlinks '\\%^\\s*" + \
                    keys + "%d' .ssh/authorized_keys ; then " + \
                    " rm -f .ssh/authorized_keys__;echo ssh key dropped ; fi \""
                if Session.testmode:
                    sshCommand = "echo " + sshCommand
                childStatus, l = self.runCommand( sshCommand, serialize, None )
                childOutput.extend(l)

            updateLock.acquire()
            status[self.cell] = childStatus
            output[self.cell] = childOutput
            updateLock.release()
            if verbose : print("...exiting thread for %s status: %d" % (self.cell, childStatus))
            return

        def runCommand( self, sshCommand, serialize, inputLines ):
            """
            Run a command in a subprocess and return its status and output lines.

            Input command is string to be executed via ssh on each cell.
            Input serialize is true if serial execution required.
            Input lines are provided if the command will read input (e.g. expect)
            ssh (or scp) command is run is a subprocess.  Stdout and stderr are
            collected.  This routine waits for completion of the subprocess and
            returns the completion code and any output lines.
            """

            tmpBannerFile = ""
            tmpBannerFd = None
            lwbanner = []
            banner_or_err = []
            def retry_if_needed(banner_or_err, stdout):
                """
                Helper function to see if this ssh command needs a retry.
                Return true if retry is needed.
                """
                # we will do a SSH retry only if --key-with-one-password option
                # presents and host has offending key
                if not konepw:
                    return False

                offend_key_exists = False
                for line in banner_or_err:
                    if "Offending key" in line or "Offending ECDSA key" in line:
                        offend_key_exists = True
                for line in stdout:
                    if "Offending key" in line or "Offending ECDSA key" in line:
                        offend_key_exists = True

                if offend_key_exists:
                    return True
                return False
            # end of retry_if_needed

            # bug 37694117: For -k option, when typing in wrong password,
            # customers want an instant print "Permission Denied".
            # So we let SSH interact directly with the terminal for password
            # prompts, instead of storing all error messages and print them
            # in the end.
            if pushKey and not konepw and not inputLines:
                try:
                    # This bash command achieves three things:
                    # 1. Runs SSH while preserving password prompts
                    # 2. Adds hostname prefix to all output lines
                    # 3. Preserves the original SSH exit status
                    bash_command = "bash -c '" + \
                        "{0}".format(
                            sshCommand.replace(" 2>&1", "").replace("'",
                                                                    "'\\''")
                        ) + \
                        " 1> >(sed \"s/^/" + \
                        self.cell.replace('"', '\\"') + \
                        ": /\") " + \
                        "2> >(sed \"s/^/" + \
                        self.cell.replace('"', '\\"') + \
                        ": /\" >&2); " + \
                        "exit ${PIPESTATUS[0]}'"
                    
                    if verbose: print("execute: %s " % bash_command)
                    
                    # Execute the command directly
                    status = subprocess.call(bash_command, shell=True)
                    
                    if status != 0:
                        # Authentication failed
                        if self.cell not in badCells:
                            badCells.append(self.cell)
                    
                    return status, []
            
                except Exception as e:
                    if verbose: print("Error executing SSH: %s" % str(e))
                    if self.cell not in badCells:
                        badCells.append(self.cell)
                    return 1, []
            # End of execution with -k option enabled.

            try:
                tmpBannerFd, tmpBannerFile = tempfile.mkstemp(
                    suffix="."+self.cell, prefix="banner_")
            except OSError as e:
                if e.errno == 24:
                    print("Error: Too many hosts to execute command in parallel"
                          ". Please rerun dcli with --batchsize option.")
                else:
                    raise
                os._exit(1)
            tmpFd = os.fdopen(tmpBannerFd, "r+")
            sshCommand += " 2>"+tmpBannerFile

            if verbose : print("execute: %s " % sshCommand)
            status = 0

            def execute_ssh_command():
                """Helper function to execute the ssh command."""
                isDefaultDecodingStandard = True
                try:
                    if os.name == "posix":
                        child = subprocess.Popen(sshCommand,
                                                shell=True,
                                                stdin=subprocess.PIPE,
                                                stdout=subprocess.PIPE,
                                                stderr=subprocess.PIPE,
                                                universal_newlines=True,
                                                close_fds=True)
                    else:
                        child = subprocess.Popen(sshCommand,
                                                shell=True,
                                                stdin=subprocess.PIPE,
                                                stdout=subprocess.PIPE,
                                                stderr=subprocess.PIPE,
                                                universal_newlines=True)

                    self.child = child
                    w = child.stdin
                    if w is not None and inputLines:
                        for l in inputLines:
                            w.write(l + "\n")
                        w.flush()

                # Communicate will help identify if a child has ended due to
                # timeout

                # The --timeout option is designed to ensure graceful
                # termination of both the remote process and the local
                # WorkThread. This is achieved by:
                # 1. Adding a timeout to the execution command on the remote
                # node.
                # 2. The remote node acknowledges the timeout, terminates its
                # process, returns the result, and signals the WorkThread to
                # terminate.
                # However, a corner case occurs when the remote node becomes
                # unresponsive and fails to acknowledge the timeout. This
                # leads to:
                # - The local WorkThread not receiving a termination signal.
                # - The WorkThread hanging indefinitely.
                # To address this issue:
                # - A 2-second grace period is added after the timeout.
                # - If no response is received during this period, the
                # WorkThread is forcefully terminated to prevent hanging.
                # This mechanism is supported starting with python 3.3

                    if (options.timeout and sys.version_info >= (3,3)):
                        try:
                            stdout, _ = child.communicate(timeout
                                                          =options.timeout+2)
                        except subprocess.TimeoutExpired:
                            sys.exit(1)
                    else:
                        stdout, _ = child.communicate()
                except UnicodeDecodeError:
                # Attempt to decode with a fallback encoding (latin-1)
                    try:
                        if os.name == "posix":
                            child = subprocess.Popen(sshCommand,
                                                     shell=True,
                                                     stdin=subprocess.PIPE,
                                                     stdout=subprocess.PIPE,
                                                     stderr=subprocess.PIPE,
                                                     universal_newlines=True,
                                                     close_fds=True,
                                                     encoding="latin-1")
                        else:
                            child = subprocess.Popen(sshCommand,
                                                     shell=True,
                                                     stdin=subprocess.PIPE,
                                                     stdout=subprocess.PIPE,
                                                     stderr=subprocess.PIPE,
                                                     universal_newlines=True,
                                                     encoding="latin-1")

                        if (options.timeout and sys.version_info >= (3,3)):
                            try:
                                stdout, _ = child.communicate(timeout
                                                          =options.timeout+2)
                            except subprocess.TimeoutExpired:
                                sys.exit(1)
                        else:
                            stdout, _ = child.communicate()
                        isDefaultDecodingStandard = False
                    except UnicodeDecodeError:
                        if verbose:
                            sys.stderr.write("Error: unable to decode by both "
                                             "default decoding standard and "
                                             "latin-1")
                        sys.exit(1)

                except OSError as e:
                    if e.errno == 24:
                        print("Error: Too many hosts to execute command in "
                              "parallel. Please rerun dcli with "
                              "--batchsize option.")
                    else:
                        raise
                    os._exit(1)

                # Convert it to be compatible with readNLines.
                # Python 2: Decode if necessary
                if sys.version_info[0] < 3 and isinstance(stdout, str) \
                and isDefaultDecodingStandard:
                    stdout = stdout.decode('utf-8')
                r = io.StringIO(stdout)
                l = self.readNLines(r, serialize)
                if w is not None:
                    w.close()
                return child, l, r
            # end of execute_ssh_command()

            child, l, _ = execute_ssh_command()
            status = child.wait()
            banner_or_err = self.readBannerOrError(tmpFd)
            tmpFd.close()
            os.unlink(tmpBannerFile)
            self.printBannerOrError(banner_or_err)

            # Check if the command has timed out
            # (return code 124 indicates timeout)
            if child.returncode == 124:
                l.append("Timeout expired for pid %d to %s...\n" % (child.pid,
                         self.cell))
            # regardless of returncode, if --key-with-one-password option
            # exists and offending key warning exists, then retry.
            elif retry_if_needed(banner_or_err, l):
                cells_need_retry.append(self.cell)

            elif child.returncode == 255:
                badCells.append(self.cell)


            if self.output_truncated == 1 and child.poll() == None:
                # stop child process since it is still running
                sys.stderr.write("Killing child pid %d to %s...\n" %
                                 (child.pid, self.cell))
                os.kill(child.pid, signal.SIGTERM)
                t = 2.0  # max wait time in secs
                while child.poll() == None:
                    if t > 0.4:
                        t -= 0.20
                        time.sleep(0.20)
                    else:  # still there, force kill
                        os.kill(child.pid, signal.SIGKILL)
                        break

            try:
                if command:
                    if status == 255:
                        if verbose:
                            self.printBannerOrError(banner_or_err)
                    else:
                        if showBanner:
                            lwbanner = self.readLinesWithBanner(l,banner_or_err)
                            l = lwbanner
                else:
                    if status != 0:
                        self.printBannerOrError(banner_or_err)

                if self.output_truncated == 1:
                    status = 1
            except OSError as e:
                # os error 10 (no child process) is ok
                if e.errno ==10:
                    if verbose : print("No child process %d for wait" %
                                       child.pid)
                else:
                    raise

            return status, l

        def readBannerOrError(self, bannerfd):
            """
             Read ssh or scp's stderr from a file.
             bannerfd is the file descriptor of file into which
             the banner or ssh/scp's stderr was written to
            """
            banner_or_err = []
            for l in iter(bannerfd.readline,""):
              banner_or_err.append(l)
            return banner_or_err

        def printBannerOrError(self, bannerOrError):
            """
             print ssh/scp's stderr. This can be the
             remote node's banner (if ssh is successful) OR
             error info (if ssh/scp is not successful)
            """
            for i in bannerOrError:
                print(self.cell +":" + i)

        def readLinesWithBanner(self, r, banner):
            """
             print the output lines from ssh command with
             remote node's banner if showbanner option is
             specified.
            """
            lines_with_banner = []
            lines_with_banner.append("******BANNER******")
            lines_with_banner.extend(banner)
            lines_with_banner.append("******BANNER******")
            lines_with_banner.extend(r)
            return lines_with_banner

        def readNLines(self, r, serialize):
            """
            Read up to maxLines; display output if max has been reached.

            Input stdout pipe of child process.
            Input serialize is true if serial execution required.
            Input gets the banner of remote node. Contents are null by default
                  --showbanner option unhides the banner
            returns any output lines not yet displayed.
            """
            i = 0
            outputLines = []

            if serialize or len(cells) == 1:
                display_chunks = 1
            else:
                display_chunks = 0

            for l in iter(r.readline, ""):
               outputLines.append(l)
               i += 1
               if i > maxLines:
                   my_cell = {}
                   my_status = {}
                   my_output = {}
                   myStatus = 0
                   myOutput = []
                   myOutput.extend(outputLines)
                   my_status[self.cell] = myStatus
                   my_output[self.cell] = myOutput
                   my_cell[self.cell] = self.cell
                   listResults( my_cell, my_status, my_output,
                                options.listNegatives, options.regexp,
                                options.preserveSpaces )
                   i = 0
                   outputLines = []
                   if display_chunks == 1:
                       continue
                   else:
                       sys.stderr.write("\nError: " + self.cell +\
                           " is returning over " + str(maxLines) +\
                           " lines; output is truncated !!!\n")
                       sys.stderr.write("Command could be retried with" +\
                           " the serialize option: --serial\n")
                       self.output_truncated = 1
                       break
            return outputLines

        def isAlive(self):
            """
            Thread isAlive() wrapper to handle name change.
            Starting in Python version 3.9, isAlive() becomes is_alive()
            """
            if sys.version_info >= (3,9):
                return super(WorkThread,self).is_alive()
            else:
                return super(WorkThread,self).isAlive()


    #end of method and WorkThread class

    # Prepare and spawn threads to SSH to cells
    output = {}
    status = {}
    waitList = []

    if ((command or exec_file) and not Session.testmode and
        not os.path.exists(SSH)):
        raise Error ( "SSH program does not exist: %s " % SSH )
    elif ((copyfiles or exec_file) and not Session.testmode and
          not os.path.exists(SCP)):
        raise Error ( "SCP program does not exist: %s " % SCP )

    copy_or_exec_file = copyfiles or exec_file
    if copy_or_exec_file:

        destname = ""

        if copyfiles:
           for a_file in copyfiles:
               files.append(a_file.strip())

        if exec_file:
           file_exec = exec_file.strip()
           files.append(file_exec)
           basename = os.path.basename(file_exec)
           destname = basename

        if destfile:
           destname = destfile

        if not (os.path.isabs(destname) or
                destname.startswith((r"\~",r"\$"))):
            if rootWithExaTmp:
                destname = EXA_TMP_DIR + destname
            else:
                destname = "./" + destname

        if exec_file:
            # an exec file can be copied to a directory or copied to a file
            # with a different name.

            command = "("
            if rootWithExaTmp:
                command += "cd "+EXA_TMP_DIR+";"
            if exec_file.strip().endswith(".scl"):
                command += "if [[ -d " + destname + " ]]; then cellcli -e @" +\
                          destname + "/" + basename + " ; else cellcli -e @" +\
                          destname + " ; fi"
            else:
                command += "if [[ -d " + destname + " ]]; then " + \
                          destname + "/" + basename + " ; else " +\
                          destname + " ; fi"

            command += ")"
            if hideStderr:
              command += " 2>/dev/null"
            else:
              command += " 2>&1"

    # enclose command in single quotes so shell does not interpret arguments
    # pre-existing single quotes must be escaped to survive
    # if quotes aready exist then don't change it
    if command and not (re.match("^'.*'$", command)
                        or re.match("^\".*\"$", command)):
        command = command.replace("'","'\\''")
        command = "'" + command + "'"

    def run_workThread(all_cells):
        for cell in all_cells:
            cellThread = WorkThread( cell )
            cellThread.start()
            if serialize:
                while cellThread.isAlive():
                    cellThread.join(1)
            else:
                waitList.append(cellThread)

        for thread in waitList:
            #we must use time'd join to allow keyboard interrupt
            while thread.isAlive():
                thread.join(1)
        # end of run_workThread

    def remove_offending_keys(cells_need_retry):
        cells_able_to_retry = []

        for cell in cells_need_retry:
            returncode = -1
            ip_address = None
            devnull = None
            is_able_to_retry = False
            # Create to suppress both stdout and stderr when ssh-keygen -R
            devnull = open(os.devnull, 'w')
            known_hosts_file = os.path.expanduser("~/.ssh/known_hosts")
            try:
                ip_address = socket.gethostbyname(cell)
            except socket.gaierror as e:
                if verbose:
                    print("Failed to resolve %s: %s" % (cell, e))
            try:
                with open(known_hosts_file, 'r') as f:
                    known_hosts_content = f.read()

                if cell in known_hosts_content:
                    # Remove the offending entry from known_hosts
                    returncode = subprocess.call(["ssh-keygen", "-R",
                                                    cell],
                                                    stdout=devnull,
                                                    stderr=devnull)
                    if returncode == 0 and verbose:
                        print("Removed %s from known_hosts and retrying "\
                                "command." % cell)
                    is_able_to_retry = True
                if ip_address and ip_address in known_hosts_content:
                    # Remove the offending entry from known_hosts
                    returncode = subprocess.call(["ssh-keygen", "-R",
                                                    ip_address],
                                                    stdout=devnull,
                                                    stderr=devnull)
                    if returncode == 0 and verbose:
                        print("Removed %s from known_hosts and retrying "\
                                "command." % ip_address)
                    is_able_to_retry = True
                if is_able_to_retry:
                    cells_able_to_retry.append(cell)

            except IOError:
                if verbose:
                    print("known_hosts doesn't exist, no retry needed.")
                pass
        return cells_able_to_retry


    try:
        run_workThread(cells)

        if cells_need_retry:
            cells_to_retry = remove_offending_keys(cells_need_retry)
            run_workThread(cells_to_retry)


    except KeyboardInterrupt:
        print("Keyboard interrupt")
        for thread in waitList:
            if thread.isAlive() and thread.child:
                try:
                    print("killing child pid %d..." % thread.child.pid)
                    os.kill(thread.child.pid, signal.SIGTERM)
                    t = 2.0  # max wait time in secs
                    while thread.child.poll() == None:
                        if t > 0.4:
                            t -= 0.20
                            time.sleep(0.20)
                        else:  # still there, force kill
                            os.kill(thread.child.pid, signal.SIGKILL)
                            time.sleep(0.4)
                            thread.child.poll() # final try
                            break
                except OSError as e:
                    if e.errno != 3:
                        # errno 3 .."no such process" ... is ok
                        raise
#           we should call join to cleanup threads but it never returns
#           thread.join(5)  --- this never returns after ctrl-c
        raise KeyboardInterrupt

    return status, output, badCells

def getInt( num_str ):
    """
    Convert string to number.  Return None if string is not a number
    """
    try:
        num = int(num_str)
    except ValueError:
        return None
    return num

def listResults( cells, statusMap, outputMap, listNegatives, regexp,
                 preserveSpaces ):
    """
    list result output from cells.

    listNegatives option restricts output by listing only lines from
    cells which returned non-zero status from copy or command execution.
    regexp option restricts output by filtering-out lines which match a
    regular expression.
    preserveSpaces option will preserve spaces at beginning of output lines.
    We print output in "cells" order which is order given in user group
    file and command line cell list.
    """
    if listNegatives :
        okCells = []
        for cell in cells:
            if cell in list(statusMap.keys()) and statusMap[cell] == 0:
                okCells.append(cell)
        if len(okCells) > 0:
            print("OK: %s" % okCells)

    compiledRE = None
    if regexp:
        reCells = []
        compiledRE = re.compile(regexp)
        for cell in cells:
            if cell in list(outputMap.keys()):
                output = outputMap[cell]
                for l in output:
                    if compiledRE.match(l.strip()):
                        reCells.append(cell)
                        break
        if len(reCells) > 0:
            print("%s: %s" % (regexp, reCells))

    for cell in cells:
        if cell in list(outputMap.keys()):
            if not listNegatives or statusMap[cell] > 0:
                output = outputMap[cell]
                for l in output:
                    # 37039888 - Python 2: Decode if necessary
                    # OK!  pylint: disable=undefined-variable
                    if sys.version_info[0] < 3 and isinstance(l, unicode):
                      l = l.encode('utf-8')
                    if not compiledRE or not compiledRE.match(l.strip()):
                        if preserveSpaces:
                            print("%s: %s" % (cell, l.rstrip()))
                        else:
                            print("%s: %s" % (cell, l.strip()))

def listVmstatHeader(headers, maxLenCellName, header1Widths, header2Widths):
    """
    print two vmstat headers aligned according to field widths
    """
    print("%s %s" % (" ".rjust(maxLenCellName),
                     listVmstatLine(header1Widths, headers[0].split())))
    print("%s:%s" %  (time.strftime('%X').rjust(maxLenCellName),
                       listVmstatLine(header2Widths, headers[1].split())))

def listVmstatLine( widths, values ):
    """
    return one line of vmstat values right justified in fields of max widths
    """
    result = ""
    i = -1
    for v in values:
        i += 1
        result += "%s " % str(v).rjust(widths[i])
    return result

def listVmstatResults( cells, outputMap, vmstatOps, count):
    """
    display results for the vmstat option.

    header lines are displayed unless suppressed by -n option.
    fields are aligned using the widest value in the output.
    Minimum, Maximum, and Average rows are added if there is more than
    one row of values.
    """
    MINIMUM = "Minimum"
    MAXIMUM = "Maximum"
    AVERAGE = "Average"

    minvalues = []
    maxvalues = []
    #approximate field widths for vmstat... these are minimums
    #           procs   memory  swap  io   system  cpu
    header1Widths = [5,   27,     9,   11,  11,     14 ]
    fieldWidths = [2,2, 6,6,6,6,   4,4,  5,5, 5,5,    2,2,2,2,2]
    total = []

    # use local time as max name width (it's used in header2)
    maxLenCellName = len(time.strftime('%X'))
    outputCount = len(list(outputMap.keys()))
    for cell in list(outputMap.keys()):
        if maxLenCellName < len(cell):
            maxLenCellName = len(cell)
        output = outputMap[cell]
        values = output[-1].split()
        i = -1
        for v in values:
            i += 1
            vInt = getInt(v)
            if vInt == None:
                continue
            if len(minvalues) <= i:
                minvalues.insert(i, vInt)
            elif minvalues[i] > vInt:
                minvalues[i] = vInt

            if len(maxvalues) <= i:
                maxvalues.insert(i, vInt)
            elif maxvalues[i] < vInt:
                maxvalues[i] = vInt
            if len(total) <= i :
                    total.insert(i, 0)
            total[i] += vInt
            if len(fieldWidths) == i :
                fieldWidths.insert(i, len(v))
            elif fieldWidths[i] < len(v):
                fieldWidths[i] = len(v)

    maxLenCellName = max([maxLenCellName, len(MAXIMUM), len(MINIMUM), len(AVERAGE)])
    # if not -n then print the header each time
    # with -n we only print on first invocation
    if count == 0 or vmstatOps.find("-n") == -1 :
        listVmstatHeader(list(outputMap.values())[0], maxLenCellName, header1Widths, fieldWidths )

    # list the output in key order, followed by min, max, and average
    for cell in cells:
        if cell in list(outputMap.keys()):
            output = outputMap[cell]
            values = output[-1].split()
            print("%s:%s" % (cell.rjust(maxLenCellName), listVmstatLine(fieldWidths, values)))

    if outputCount > 1:
        print("%s:%s" % (MINIMUM.rjust(maxLenCellName), listVmstatLine(fieldWidths, minvalues)))
        print("%s:%s" % (MAXIMUM.rjust(maxLenCellName), listVmstatLine(fieldWidths, maxvalues)))
        avgvalues = []
        for v in total:
            avgvalues.append( int((v/outputCount)+0.5) )
        print("%s:%s" % (AVERAGE.rjust(maxLenCellName), listVmstatLine(fieldWidths, avgvalues)))

def get_file_descriptor_limit(verbose):
    """
    Retrieve the current system file descriptor limit using the 'ulimit -n'
    command.

    Args:
        verbose (bool): If True, prints debug information about the command
        output.

    Returns:
        int: The system file descriptor limit.
             Returns None if retrieval fails.
    """
    try:
        # Use bash explicitly to ensure consistent environment
        process = subprocess.Popen(['bash', '-c', 'ulimit -n'],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        output, _ = process.communicate()

        if process.returncode != 0:
            if verbose:
                print("ulimit command failed")
            return None

        # Handle bytes vs string for Python 2 and 3
        if isinstance(output, bytes):
            output = output.decode('utf-8')

        # Clean the output and convert to integer
        limit = int(str(output).strip())

        if verbose:
            print("Parsed ulimit value: %d" % limit)

        return limit
    except (OSError, ValueError) as e:
        if verbose:
            print("Error getting ulimit: %s" % str(e))
        return None

def update_max_threads(options):
    """
    Update options.maxThds based on the system's file descriptor limit.

    Uses 7 FDs per cell calculation:
    - 5 FDs for SSH (stdin, stdout, stderr, network socket, control socket)
    - 1 FD for temporary banner file
    - 1 FD buffer for safety

    Args:
        options: The options object containing maxThds and other settings

    Returns:
        bool: True if maxThds was updated, False otherwise
    """
    # We won't add --batchsize option if --batchsize or --serial is enabled.
    if options.maxThds != 0 or options.serializeOps:
        return False

    fd_limit = get_file_descriptor_limit(options.verbosity)
    if fd_limit is None:
        return False

    # Calculate max threads based on FD limit
    # Reserve ~10 FDs for the Python process itself
    FDS_PER_CELL = 7  # 5 for SSH + 1 for banner + 1 buffer
    RESERVED_FDS = 10

    max_threads = (fd_limit - RESERVED_FDS) // FDS_PER_CELL

    if max_threads < 1:
        if options.verbosity:
            print("Calculated thread limit too low: %d" % max_threads)
        return False

    if options.verbosity:
        print("Setting maxThds to %d based on fd limit %d" %
              (max_threads, fd_limit))

    options.maxThds = max_threads
    return True


def main(argv=None):

    """
    Main program.

    This builds the option handler and handles help and usage errors.
    Then calls buildCommand to build the command to be sent.
    Then calls buildCellList to build a list of cells to connect with.
    Then calls copyAndExecute to send or execute commands to all cells.
    Then calls listResults to optionally abbreviate and list output
    Finally it returns 0, 1, or 2 based on results.
    """
    if argv is None:
        argv = sys.argv
    elif argv[0].startswith("test"):
        # tests cannot rely on ssh ports
        Session.testmode = "test"

    usage = "usage: %prog [options] [command]"
    parser = OptionParser(usage=usage, add_help_option=False,
                          version="version %s" % version)
    parser.add_option("--batchsize",
                      action="store", type="int", dest="maxThds", default=0,
                      help="limit the number of target hosts on which to run the command" +\
                      " or file copy in parallel")
    parser.add_option("-c",
                      action="append", type="string", dest="cells",
                      help="comma-separated list of hosts")
    parser.add_option("--ctimeout",
                      action="store", type="int", dest="ctimeout",
                      help="Maximum time in seconds for initial host connect")
    parser.add_option("-d",
                      help=(
                            "Destination directory (ends with '/') or file. "
                            "Non-existent directories on the remote node will"
                            " be created automatically."
                           ),
                     action="store", type="string", dest="destfile")
    parser.add_option("-f",
                     help="files to be copied",
                     action="append", type="string", dest="file")
    parser.add_option("-g",
                     help="file containing list of hosts",
                     action="store", type="string", dest="groupfile")

    # help displays the module doc text plus the option help
    def doHelp(option, opt, value, parser):
        # option, opt, value are unused but needed for optparse and pylint
        _ = (option, opt, value)
        print( __doc__ )
        parser.print_help()
        sys.exit(0)

    parser.add_option("-h", "--help",
                     help="show help message and exit",
                     action="callback", callback=doHelp)
    parser.add_option("--hidestderr",
                     help="hide stderr for remotely executed commands in ssh",
                     action="store_true", dest="hideStderr", default=False)
    parser.add_option("-k",
                      action="store_true", dest="pushKey", default=False,
                      help="push ssh key to host's authorized_keys file. "
                           "Please note that this is done in serial and will "
                           "prompt for password for every host in the input "
                           "list. For hosts with same password, use "
                           "--key-with-one-password for parallel ssh key "
                           "setup.")
    parser.add_option("--key-with-one-password",
                      action="store_true", dest="konepw", default=False,
                      help="apply one credential for pushing ssh key to "
                           "host's authorized_keys files in parallel")
    parser.add_option("-l",
                     help="user to login as on remote hosts (default: celladmin) ",
                     action="store", type="string", dest="userID")
    parser.add_option("--root-exadatatmp",
                      action="store_true", dest="rootWithExaTmp", default=False,
                      help="root user login using directory "+ EXA_TMP_DIR)
    parser.add_option("--maxlines",
                     action="store", type="int", dest="maxLines", default=100000,
                     help="limit output lines from a host when in parallel execution over " +\
                     "multiple hosts (default: 100000)")
    parser.add_option("-n",
                      action="store_true", dest="listNegatives", default=False,
                      help="abbreviate non-error output ")
    parser.add_option("--preserve-spaces",
                      help="preserve leading spaces in output lines",
                      action="store_true", dest="preserveSpaces",
                      default=False)
    parser.add_option("--prompt",
                      action="store_true", dest="prompt", default=False,
                      help="ssh will prompt for password and hostkey " +\
                           "checking when needed")
    parser.add_option("-r",
                     help="abbreviate output lines matching a regular expression",
                     action="store", type="string", dest="regexp")
    parser.add_option("-s",
                     help="string of options passed through to ssh",
                     action="store", type="string", dest="sshOptions")
    parser.add_option("--scp",
                     help="string of options passed through to scp if different from sshoptions",
                     action="store", type="string", dest="scpOptions")
    parser.add_option("--serial",
                      action="store_true", dest="serializeOps", default=False,
                      help="serialize execution over the hosts")
    parser.add_option("--showbanner",
                     help="show banner of the remote node in ssh",
                     action="store_true", dest="showBanner", default=False)
    parser.add_option("-t",
                      action="store_true", dest="list", default=False,
                      help="list target hosts ")
    parser.add_option("--timeout",
                      action="store", type="float", dest="timeout",
                      help="Maximum time in seconds for command execution")
    parser.add_option("--unkey",
                      action="store_true", dest="dropKey", default=False,
                      help="drop keys from target hosts' authorized_keys file")
    parser.add_option("-v", action="count", dest="verbosity",
                      help="print extra messages to stdout")
    parser.add_option("--vmstat",
                      help="vmstat command options",
                      action="store", type="string", dest="vmstatOps")
    parser.add_option("-x",
                      help="file to be copied and executed",
                      action="store", type="string", dest="exec_file")

    # stop parsing when we hit first arg to allow unquoted commands
    parser. disable_interspersed_args()
    (options, args) = parser.parse_args(argv[1:])

    # split options.file if there are list items
    if options.file:
       options_file=[]
       for item_file in options.file:
           options_file.extend(item_file.split())

       options.file = options_file

    # trim exec file option
    if options.exec_file:
       options.exec_file=options.exec_file.strip()

    if options.verbosity :
        print('options.cells: %s' % options.cells)
        print('options.ctimeout: %s' % options.ctimeout)
        print('options.destfile: %s' % options.destfile)
        print('options.file: %s' % options.file)
        print('options.group: %s' % options.groupfile)
        print('options.hideStderr: %s' % options.hideStderr)
        print('options.rootWithExaTmp: %s' % options.rootWithExaTmp)
        print('options.maxLines: %s' % options.maxLines)
        if options.maxThds != 0:
            print('options.maxThds: %s' % options.maxThds)
        print('options.listNegatives: %s' % options.listNegatives)
        print('options.pushKey: %s' % options.pushKey)
        print('options.konepw: %s' % options.konepw)
        print('options.regexp: %s' % options.regexp)
        print('options.sshOptions: %s' % options.sshOptions)
        print('options.showBanner: %s' % options.showBanner)
        print('options.scpOptions: %s' % options.scpOptions)
        print('options.dropKey: %s' % options.dropKey)
        print('options.serializeOps: %s' % options.serializeOps)
        print('options.userID: %s' % options.userID)
        print('options.verbosity %s' % options.verbosity)
        print('options.vmstatOps %s' % options.vmstatOps)
        print('options.exec_file: %s' % options.exec_file)
        print("argv: %s" % argv)

    returnValue = 0
    try:
        command = None

        if len(args) > 0:
            command = buildCommand( args, options )

        if not command and not (options.list or options.exec_file
                                or options.file or options.pushKey
                                or options.konepw
                                or options.dropKey
                                or options.vmstatOps != None):
            raise UsageError("No command specified.")
        if command and options.exec_file:
            raise UsageError("Cannot specify both command and exec file")
        if options.file and options.exec_file:
            raise UsageError("Cannot specify both copy file and exec file")

        if (options.hideStderr) and (len(args) == 0):
            raise UsageError("hidestderr(--hi) option is only used when remote"\
                             " command is specified")
        if options.listNegatives and options.regexp:
            raise UsageError("Cannot specify both non-error and regular "\
                             "expression abbrevation options")
        vmstatCount = None
        # an empty option value is is ok for vmstat
        if options.vmstatOps != None and options.vmstatOps == "":
            options.vmstatOps = " "
        if options.vmstatOps :
            if (options.exec_file or options.file or command):
                raise UsageError("Cannot specify vmstat option with copy file,"\
                                 " exec file, or command")
            if (options.listNegatives or options.regexp):
                raise UsageError("Cannot specify vmstat option with abbreviate"\
                                 " options")
            vmstatCount, command = checkVmstat(options.vmstatOps)
            if vmstatCount == None:
                command = "vmstat " + options.vmstatOps
        if (options.pushKey or options.konepw or options.dropKey):
            checkKeys(options.prompt, options.verbosity)
        if options.ctimeout is not None:
            if options.ctimeout < 0:
                raise UsageError("--ctimeout value must be a positive"\
                                 " number")
        if options.userID and options.rootWithExaTmp:
            if options.userID != "root":
                raise UsageError("root-exadatatmp implies root user. It cannot be used with other user IDs")

        clist = buildCellList( options.cells, options.groupfile )

        batch = False
        if options.maxThds != 0:
            if options.serializeOps:
                raise UsageError("Cannot specify both serial mode and batch mode")
            if options.maxThds < 1:
                raise UsageError("Cannot specify batchsize less than 1")
            batch = True

        # Bug 37356088: Auto-configure maxThds based on file descriptor limit
        # if not explicitly set
        if len(clist) > 1 and update_max_threads(options):
            batch = True
            if options.verbosity:
                print("Automatically configured batch size to %d"
                      % options.maxThds)

        if len(clist) == 0 :
            raise UsageError("No hosts specified.")

        if options.exec_file:
            checkFile(options.exec_file, True )
        if options.file:
           for item_file in options.file:
               checkFile(item_file, False )
        if options.destfile and not (options.exec_file or options.file):
            raise UsageError("Cannot specify destination without copy file or exec file")
        if options.list:
            print("Target hosts: %s" % clist)

        if (command or options.exec_file or options.file or
            options.pushKey or options.konepw or options.dropKey):

            if options.verbosity and len(clist) > 0 :
                print("Connecting to hosts: %s" % clist)

        if (options.konepw and len(clist) > 0 ):
            getOnePw()
        if len(clist) > 0 :
            batchBegin = 0
            sampleCount = 1
            loopCount = 0
            while True:
                if ( options.maxThds == 0 or
                     (options.maxThds >= len(clist) - batchBegin)):
                    batchEnd = len(clist)
                else:
                    batchEnd = batchBegin + options.maxThds
                cells = clist[batchBegin:batchEnd]
                if vmstatCount != None :
                    # For vmstat, do periodic sampling of vmstat and print as we go.
                    # the first time through the loop we retrieve just the boot stats
                    # thereafter we retrieve a delayed sample (sampleCount =2)
                    while True:
                        statusMap, outputMap, badCells = copyAndExecute(
                                               cells, None, None, None,
                                               (command or "") +
                                               str(sampleCount), options)
                        if len(badCells) > 0 :
                            returnValue = 1
                            sys.stderr.write("Unable to connect to hosts: %s\n" %\
                                     badCells)
                        if max( statusMap.values() ) > 0 :
                            #error returned  ... display results in usual fashion and exit
                            listResults( clist, statusMap, outputMap, None,
                                         None, options.preserveSpaces )
                            break
                        listVmstatResults( clist, outputMap, options.vmstatOps,
                                           loopCount)
                        if batch: break
                        if vmstatCount >= 0 :
                            loopCount += 1
                            if loopCount >= vmstatCount :
                                break
                        sampleCount = 2
                else:
                    statusMap, outputMap, badCells = copyAndExecute(
                                                           cells,
                                                           options.file,
                                                           options.exec_file,
                                                           options.destfile,
                                                           command, options)
                    if len(badCells) > 0 :
                        returnValue = 1
                        sys.stderr.write("Unable to connect to hosts: %s\n" %\
                                     badCells)
                    listResults( clist, statusMap, outputMap,
                                 options.listNegatives, options.regexp,
                                 options.preserveSpaces )

                values = list(statusMap.values()) + [returnValue]
                returnValue = max( values )
                if batchEnd == len(clist):
                    loopCount += 1
                    if batch and vmstatCount is not None and (vmstatCount < 0 or loopCount < vmstatCount):
                        batchBegin = 0
                        sampleCount = 2
                    else:
                        break
                else:
                    batchBegin = batchEnd

    except UsageError as err:
        sys.stderr.write("Error: %s\n" % err.msg)
        parser.print_help()
        # parser.error(err.msg) -- doesn't print usage options.
        return 2

    except Error as err:
        sys.stderr.write("Error: %s\n" % err.msg)
        return 2

    except IOError as err:
        sys.stderr.write("IOError: [Errno %s] %s\n" % (err.errno,err.strerror))
        return 2

    except KeyboardInterrupt:
        # sys.exit(1)  does not work after ctrl-c
        os._exit(1)

    # return 1 for any other error
    return returnValue and 1

# Main program

if __name__ == "__main__" :
    sys.exit(main())
