#!/usr/bin/env python
# coding: utf-8
import xarray as xr
import vertical_integral
import numpy as np
import os
import datetime as dt

def calculate_artmip_vertical_integrals(triplet_line,
                                        one_timestep_test = False,
                                        write_output_files = True,
                                        original_base = "/global/cscratch1/sd/cmip6/CMIP6/CMIP/",
                                        output_base = os.environ['SCRATCH'] + '/ARTMIP_CMIP6/',
                                        do_clobber = False,
                                        be_verbose = True,
                                        no_return_xarray = True,
                                       ):
    """ Calculates prw, windhusavi, uhusavi, and vhusavi on CMIP6 output.
    
        input:
        ------
        
            triplet_line     : a comma-separated string containing three fields: hus_file, ua_file, and va_file
                               These fields indicated absolute paths to concordant CMIP6 files containing humidity
                               and wind variables.  hus_file must be present, but ua_file and va_file need not be.
                               If either wind field is missing, *husavi will not be calculated.
                               
            one_timestep_test : flags whether to run on only one timestep of input; useful for testing.
                               
            write_output_files : flags whether to write output files to disk.  The following options are ignored
                                 if this is False
                               
            original_base    : the base CMIP6 directory path in the CMIP6 lines
            
            output_base      : the base path to which to output the new fields.  Defaults to $SCRATCH/ARTMIP_CMIP6/
            
            do_clobber       : flags whether to overwrite existing files; skips working if all the expected output files already exist
            
            be_verbose       : flags whether to print updates along the way
            
            no_return_xarray : flags whether to return artmip_xr
                               
            
        output:
        -------
        
            output_file_list, [artmip_xr]  : a list of files written to disk, and (optionally) an xarray.DataSet containing the calculated fields
                                             (this will be empty if write_output_files is False).  If do_clobber is False, and no
                                             files are actually written, the paths will still be returned, but artmip_xr will be None if 
                                             no_return_xarray is False.
            
            If write_output_files is True, then separate files for each calculated variable are written to disk.
            
            The files will be written to disk with the following template, which mirrors the ESMF CMIP6 directory structure.  Here, {variable} is one of
            [prw, windhusavi, uhusavi, vhusavi], and {version_string} is the version string associated with hus_file (may differ for ua_file and va_file):
            
            {output_base}/{center}/{model}/{simulation}/{ensemble}/6hrLev/{variable}/gn/{version_string}/{variable}_6hrLev_{model}_{simulation}_{ensemble}_gn_{file_id}.nc
            
            
    
    """
    
    default_chunk_size = 10
    
    def vprint(msg):
        """ Print a message only if be_verbose is True"""
        if be_verbose:
            print(msg)
    
    # extract the file paths from the triplet line
    hus_file, ua_file, va_file = triplet_line.rstrip().split(',')
    
    # set output file names
    output_file_list = []
    if write_output_files:
        
        # parse the hus file path
        output_file_template = hus_file.replace(original_base, output_base)
        output_file_template = output_file_template.replace('hus', '{variable}')
        
        # set the expected prw file name
        prw_output_file = os.path.abspath(output_file_template.format(variable = 'prw'))
        output_file_list.append(prw_output_file)
        
        # set the expected *husavi file names, if we are calculating these variables
        windhusavi_output_file = None
        uhusavi_output_file = None
        vhusavi_output_file = None
        if ua_file != "" and va_file != "":
            windhusavi_output_file = os.path.abspath(output_file_template.format(variable = 'windhusavi'))
            uhusavi_output_file = os.path.abspath(output_file_template.format(variable = 'uhusavi'))
            vhusavi_output_file = os.path.abspath(output_file_template.format(variable = 'vhusavi'))
            
            # append the file names to the output file list
            output_file_list.append(windhusavi_output_file)
            output_file_list.append(uhusavi_output_file)
            output_file_list.append(vhusavi_output_file)
            
        # if we aren't overwriting files and the expected files already exist, simply return
        if all([ os.path.exists(ofile) for ofile in output_file_list]) and not do_clobber:
            if no_return_xarray:
                return output_file_list
            else:
                return output_file_list, None
            
    vprint("Opening " + hus_file)
    # open the hus, ua, and va files (if ua and va are available)
    hus_xr = xr.open_dataset(hus_file,
                             decode_coords = False,
                             decode_times = False,
                            )
    # check whether the current chunk size will cause the read to overflow
    chunk_size = default_chunk_size
    if chunk_size > len(hus_xr['time']):
        chunk_size = len(hus_xr['time'])
    # trigger dask usage by chunking in time
    hus_xr = hus_xr.chunk({'time': chunk_size})
    hus_xr = xr.decode_cf(hus_xr, decode_coords = True, decode_times = True)
    
    ua_xr = None
    va_xr = None
    if ua_file != "":
        vprint("Opening " + ua_file)
        ua_xr = xr.open_dataset(ua_file,
                                decode_coords = False,
                                decode_times = False,
                               )
        # trigger dask usage by chunking in time
        ua_xr = ua_xr.chunk({'time': chunk_size})
        ua_xr = xr.decode_cf(ua_xr, decode_coords = True, decode_times = True)
    if va_file != "":
        vprint("Opening " + va_file)
        va_xr = xr.open_dataset(va_file,
                                decode_coords = False,
                                decode_times = False,
                               )
        # trigger dask usage by chunking in time
        va_xr = va_xr.chunk({'time': chunk_size})
        va_xr = xr.decode_cf(va_xr, decode_coords = True, decode_times = True)
    
    if one_timestep_test:
        hus_xr = hus_xr.isel(time = 0).load()
        if ua_xr is not None:
            ua_xr = ua_xr.isel(time = 0).load()
        if va_xr is not None:
            va_xr = va_xr.isel(time = 0).load()
    
    # calculate iwv
    vprint("Calculating IWV on {}".format(os.path.basename(hus_file)))
    artmip_xr = vertical_integral.integrate(hus_xr, variables = ['hus'])
    
    # set metadata for the vertical integral of hus
    artmip_xr = artmip_xr.rename(dict(hus = 'prw'))
    artmip_xr['prw'].attrs['long_name'] = "Integrated Water Vapor"
    artmip_xr['prw'].attrs['units'] = "kg/m2"

    # attempt to calculate ivt
    if ua_xr is not None and va_xr is not None:
        vprint("Calculating IVT on {}".format(os.path.basename(hus_file)))
        artmip_xr['uhusavi'] = ua_xr['ua'] * hus_xr['hus']
        artmip_xr['vhusavi'] = va_xr['va'] * hus_xr['hus']
        
        # integrate the components of IVT
        artmip_xr = vertical_integral.integrate(artmip_xr, variables = ['uhusavi', 'vhusavi'])
        
        # combine the components of IVT
        artmip_xr['windhusavi'] = np.sqrt(artmip_xr['uhusavi']**2 + artmip_xr['vhusavi']**2)
        
        # set metadata
        artmip_xr['windhusavi'].attrs['long_Name'] = "Integrated Vapor Transport"
        artmip_xr['windhusavi'].attrs['units'] = "kg/m/s"
        
        artmip_xr['uhusavi'].attrs['long_Name'] = "Northward Integrated Vapor Transport"
        artmip_xr['uhusavi'].attrs['units'] = "kg/m/s"
        
        artmip_xr['vhusavi'].attrs['long_Name'] = "Eastward Integrated Vapor Transport"
        artmip_xr['vhusavi'].attrs['units'] = "kg/m/s"
        
        
    # add metadata about the git repository
    artmip_xr.attrs['artmip_cmip6_source_files'] = triplet_line.rstrip()
    artmip_xr.attrs['artmip_cmip6_integral_script'] = os.path.abspath(__file__)
    artmip_xr.attrs['artmip_cmip6_integral_calculation_date'] = str(dt.datetime.today())
    try:
        import git
        _repo = git.Repo(search_parent_directories=True)
        _git_sha = _repo.head.object.hexsha
        _git_short_sha = _repo.git.rev_parse(_git_sha, short=7)
        _git_branch = _repo.active_branch
        artmip_xr.attrs['artmip_script_repo'] = "https://bitbucket.org/lbl-cascade/cmip6_artmip_integrals.git"
        artmip_xr.attrs['artmip_script_branch'] = "{}".format(_git_branch)
        artmip_xr.attrs['artmip_script_rev'] = "{}".format(_git_short_sha)
    except:
        pass
    
    if write_output_files:
        
        # turn off fill values
        fill_value = 1e20
        unlimited_dims = ["time"]
        
        def fix_fill_values(ds, variable):
            """ Fix fill values in xarray output"""
            for var in ds.variables:
                if var == variable:
                    ds[var].encoding['_FillValue'] = fill_value
                else:
                    ds[var].encoding['_FillValue'] = None 
        
        def ensure_output_dir_exists(output_file):
            output_dir = os.path.dirname(output_file)
            os.makedirs(output_dir, exist_ok = True)
        
        # write the prw file
        if not os.path.exists(prw_output_file) or do_clobber:
            # extract only the specific variable
            if 'windhusavi' in artmip_xr:
                prw_xr = artmip_xr.drop(['windhusavi', 'uhusavi', 'vhusavi'])
            else:
                prw_xr = artmip_xr
            # make sure the output directory exists
            ensure_output_dir_exists(prw_output_file)
            # deal with fill values
            fix_fill_values(prw_xr, "prw")
            # write the prw file
            vprint("Writing " + prw_output_file)
            prw_xr.to_netcdf(prw_output_file,
                             engine = 'h5netcdf',
                             unlimited_dims = unlimited_dims)
            prw_xr.close()
            
            
        # write the windhusavi file
        if 'windhusavi' in artmip_xr.variables:
            if not os.path.exists(windhusavi_output_file) or do_clobber:
                # extract only the specific variable
                windhusavi_xr = artmip_xr.drop(['prw', 'uhusavi', 'vhusavi'])
                # make sure the output directory exists
                ensure_output_dir_exists(windhusavi_output_file)
                # write the windhusavi file
                # deal with fill values
                fix_fill_values(windhusavi_xr, "windhusavi")
                vprint("Writing " + windhusavi_output_file)
                windhusavi_xr.to_netcdf(windhusavi_output_file,
                                        engine = 'h5netcdf',
                                        unlimited_dims = unlimited_dims)
                windhusavi_xr.close()
                
        # write the uhusavi file
        if 'uhusavi' in artmip_xr.variables:
            if not os.path.exists(uhusavi_output_file) or do_clobber:
                # extract only the specific variable
                uhusavi_xr = artmip_xr.drop(['prw', 'windhusavi', 'vhusavi'])
                # make sure the output directory exists
                ensure_output_dir_exists(uhusavi_output_file)
                # deal with fill values
                fix_fill_values(uhusavi_xr, "uhusavi")
                # write the uhusavi file
                vprint("Writing " + uhusavi_output_file)
                uhusavi_xr.to_netcdf(uhusavi_output_file,
                                     engine = 'h5netcdf',
                                     unlimited_dims = unlimited_dims)
                uhusavi_xr.close()
                
        # write the vhusavi file
        if 'vhusavi' in artmip_xr.variables:
            if not os.path.exists(vhusavi_output_file) or do_clobber:
                # extract only the specific variable
                vhusavi_xr = artmip_xr.drop(['prw', 'windhusavi', 'uhusavi'])
                # make sure the output directory exists
                ensure_output_dir_exists(vhusavi_output_file)
                # deal with fill values
                fix_fill_values(vhusavi_xr, "vhusavi")
                # write the vhusavi file
                vprint("Writing " + vhusavi_output_file)
                vhusavi_xr.to_netcdf(vhusavi_output_file,
                                     engine = 'h5netcdf',
                                     unlimited_dims = unlimited_dims)
                vhusavi_xr.close()
                
        # close input files to avoid netCDF file handle limit issues
        hus_xr.close()
        if ua_xr is not None:
            ua_xr.close()
        if va_xr is not None:
            va_xr.close()
   
    vprint("Done with {}".format(os.path.basename(hus_file)))
    
    if no_return_xarray:
        return output_file_list
    else:
        return output_file_list, artmip_xr
    
