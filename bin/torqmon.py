#!/usr/bin/env python


#
# Monitors Torque and Maui for job information
#


if __name__ == "main":
    from sysmon.daemon import Daemon # Creates the standard two fork daemon
    