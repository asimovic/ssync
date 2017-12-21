from b2_ext.api import B2Api
from index.secure_index import SecureIndex


def ValidateSecureIndex(secIndex: SecureIndex, api: B2Api):
    #pull all names from api
    #validate them agains the index
    #if different report error, reupload or delete
    return True