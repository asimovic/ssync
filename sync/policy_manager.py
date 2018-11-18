######################################################################
#
# File: sync/policy_manager.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
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
            if args.keep:
                return UpPolicy
            else:
                return UpAndDeletePolicy
        elif syncType == SyncType.DOWNLOAD:
            if args.keep:
                return DownPolicy
            else:
                return DownAndDeletePolicy
        assert False, f'Invalid sync type: {syncType}, args: {str(args)}'


POLICY_MANAGER = SyncPolicyFactory()
