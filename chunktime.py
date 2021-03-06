import os
import numpy as np
import xarray as xr
import json
import tempfile
import copy

import task_manager as tm

#------------------------------------------------------------
#-- function
#------------------------------------------------------------
def json_cmd(kwargs_dict):
    return '\'{0}\''.format(json.dumps(kwargs_dict))

#------------------------------------------------------------
#-- function
#------------------------------------------------------------

def ncrcat(input,output,kwargs={}):
    '''Call `ncrcat` via task_manager

    Parameters
    ----------

    input : list
      list of files to concatenate
    output : str
      output file
    kwargs : dict, optional
      dictionary of keyword arguments to task_manager.submit
    '''

    kwargs['modules'] = ['nco']
    kwargs['module_purge'] = False
    if 'memory' not in kwargs:
        kwargs['memory'] = '100GB'

    (fid,tmpfile) = tempfile.mkstemp('.filelist')
    with open(tmpfile,'w') as fid:
        for f in input:
            fid.write('%s\n'%f)

    jid = tm.submit(['cat',tmpfile,'|','ncrcat','-o',output],**kwargs)
    return jid

#------------------------------------------------------------
#-- function
#------------------------------------------------------------

def gen_time_chunks(start,stop,chunk_size):
    '''generate a list of index pairs

    Parameters
    ----------

    start : int, optional
      starting time index, default = 0
    stop : int, optional
      final time index, default = None (i.e., the last index)
    chunk_size : int
      number of time levels in time chunks
    '''

    time_level_count = stop - start
    nchunk =  time_level_count / chunk_size
    if time_level_count%chunk_size != 0:
        nchunk += 1
    time_ndx = [(start+i*chunk_size,start+i*chunk_size+chunk_size)
                for i in range(nchunk-1)] + \
                [(start+(nchunk-1)*chunk_size,stop)]

    return time_ndx

#------------------------------------------------------------
#-- function
#------------------------------------------------------------

def apply(script,
          kwargs,
          chunk_size,
          start=0,
          stop=None,
          clobber=False,
          cleanup=True,
          submit_kwargs_i={'memory':'30GB'},
          submit_kwargs_cat={}):
    '''run script on time segments within a file and concatenate results

    Parameters
    ----------

    script : str
      string for the executable to run
    kwargs : dict
      dictionary of keyword arguments; must contain "file_in" and "file_out"
    chunk_size : int
      number of time levels in time chunks
    start : int, optional
      starting time index, default = 0
    stop : int, optional
      final time index, default = None (i.e., the last index)
    clobber : logical, optional
      overwrite "file_out"
    cleanup : logical, optional
      remove intermediate files after completion
    submit_kwargs : dict, optional
      dictionary of keyword arguments to task_manager.submit
    submit_kwargs_cat : dict, optional
      dictionary of keyword arguments to task_manager.submit for ncrcat

    Returns: jid_list : list of job ID numbers
    '''

    if 'file_in' not in kwargs:
        raise ValueError('Missing "file_in" in kwargs')

    if 'file_out' not in kwargs:
        raise ValueError('Missing "file_out" in kwargs')

    jid_list = []
    file_out = copy.copy(kwargs['file_out'])
    if os.path.exists(file_out) and not clobber:
        return jid_list

    #-- define fuction to operate on single time chunk
    def _apply_one_chunk(tnx):

        #-- intermediate output file
        file_out_i = file_out+'.tnx.%d-%d'%(tnx)

        if os.path.exists(file_out_i) and not clobber:
            return file_out_i

        #-- update input arguments
        if isel in kwargs:
            kwargs['isel'] = kwargs['isel'].update({'time':tnx})
        else:
            kwargs['isel'] = {'time':tnx}

        kwargs.update('file_out': file_out_i})

        #-- submit
        print(json_cmd(kwargs))
        jid = tm.submit([script,json_cmd(kwargs)],**submit_kwargs_i)
        jid_list.append(jid)

        return file_out_i


    #-- get stopping index
    if stop is None:
        stop = len(xr.open_dataset(kwargs['file_in'],
                                   decode_times=False,
                                   decode_coords=False).time)
    time_chunks = gen_time_chunks(start,stop,chunk_size)

    #-- operate on each chunk
    file_cat = [_apply_one_chunk(tnx) for tnx in time_chunks]

    #-- concatenate files
    submit_kwargs_cat.update({'depjob':jid_list})
    jid = ncrcat(file_cat,file_out,submit_kwargs_cat)

    #-- cleanup
    if cleanup:
        tm.submit(['rm','-f',' '.join(file_cat)],depjob=jid)

    return jid_list
