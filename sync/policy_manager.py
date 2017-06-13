######################################################################
#
# File: sync/policy_manager.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
# Modified by: Alex Simovic
#
######################################################################

from .policy import DownAndDeletePolicy, DownPolicy
from .policy import UpAndDeletePolicy, UpPolicy
from enum import Enum

class SyncType(Enum):
    UPLOAD = 1
    DOWNLOAD = 2


class SyncPolicyFactory(object):
    def createPolicy(self, sourceDir, source_file, destinationDir, dest_file,
                           syncType, now_millis, args):
        policyType = self.getPolicyType(syncType, args)
        return policyType(sourceDir, source_file, destinationDir, dest_file, now_millis, args)

    def getPolicyType(self, syncType, args):
        if syncType == SyncType.UPLOAD:
            if args.delete:
                return UpAndDeletePolicy
            else:
                return UpPolicy
        elif syncType == SyncType.DOWNLOAD:
            if args.delete:
                return DownAndDeletePolicy
            else:
                return DownPolicy
        assert False, f'Invalid sync type: {syncType}, args: {str(args)}'


POLICY_MANAGER = SyncPolicyFactory()
