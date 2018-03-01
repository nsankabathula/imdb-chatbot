import os
import ipyparallel as ipp

rc = ipp.Client(sshserver='paperspace@ps1rwu2f9:8888',password='Feb@2018')
rc.ids
#ar = rc[:].apply_async(os.getpid)
#pid_map = ar.get_dict()

