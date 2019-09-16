import xarray as xr
import numpy as np

neg_one_over_g = -1/9.806159973144531

def dpressure_from_BCC_CSM2_MR(xrd):
    """ Calculates the vertical pressure differential
    
        input:
        ------
        
            xrd    : an xarray CMIP6 dataset
            
        output:
        -------
        
            dp     : the vertical pressure differential, centered
                     on the model mid-levels
                     [Pa]
                     
    
        Model: BCC-CSM2-MR
        Formula: p = a*p0 + b*ps
    """
    
    # p = a*p0 + b*ps
    a = xrd['a_bnds']
    b = xrd['b_bnds']
    p0 = xrd['p0']
    ps = xrd['ps']
    
    da = a.isel(bnds = 1) - a.isel(bnds = 0)
    db = b.isel(bnds = 1) - b.isel(bnds = 0)
    
    dp = da*p0 + db*ps
    
    return dp
    
def dpressure_from_CESM2(xrd):
    """ Calculates the vertical pressure differential
    
        input:
        ------
        
            xrd    : an xarray CMIP6 dataset
            
        output:
        -------
        
            dp     : the vertical pressure differential, centered
                     on the model mid-levels
                     [Pa]
                     
    
        Model: CESM2
        Formula: p = a*p0 + b*ps
    """
    
    # p = a*p0 + b*ps
    a = xrd['a_bnds']
    b = xrd['b_bnds']
    p0 = xrd['p0']
    ps = xrd['ps']
    
    # this is a kludge to deal with CESM2 having data arrays input upside down
    a.values = a.values[::-1]
    b.values = b.values[::-1]
    
    da = a.isel(nbnd = 1) - a.isel(nbnd = 0)
    db = b.isel(nbnd = 1) - b.isel(nbnd = 0)
    
    dp = da*p0 + db*ps
    
    return dp
    
def dpressure_from_CNRM_CM6_1(xrd):
    """ Calculates the vertical pressure differential
    
        input:
        ------
        
            xrd    : an xarray CMIP6 dataset
            
        output:
        -------
        
            dp     : the vertical pressure differential, centered
                     on the model mid-levels
                     [Pa]
                     
    
        Model: CNRM-CM6-1
        Formula: p = ap + b*ps
    """
    
    # p = a + b*ps
    a = xrd['ap_bnds']
    b = xrd['b_bnds']
    ps = xrd['ps']
    
    # this is a kludge to deal with the fact that this model has bad values
    # for bnds = 1 for both ap and b; this calculates the bnds = 1 values
    # as the average of the mid-level values
    a.isel(bnds = 1).values[:-1] = (xrd['ap'].values[1:] + xrd['ap'].values[:-1])/2
    b.isel(bnds = 1).values[:-1] = (xrd['b'].values[1:] + xrd['b'].values[:-1])/2
    a.isel(bnds = 1).values[-1] = a.isel(bnds = 1).values[-2]
    b.isel(bnds = 1).values[-1] = b.isel(bnds = 1).values[-2]
    
    a.isel(bnds = 0).values[1:] = (xrd['ap'].values[1:] + xrd['ap'].values[:-1])/2
    b.isel(bnds = 0).values[1:] = (xrd['b'].values[1:] + xrd['b'].values[:-1])/2
    a.isel(bnds = 0).values[0] = 0
    b.isel(bnds = 0).values[0] = 1
    
    da = a.isel(bnds = 1) - a.isel(bnds = 0)
    db = b.isel(bnds = 1) - b.isel(bnds = 0)
    
    dp = da + db*ps
    
    return dp
    
def dpressure_from_CNRM_ESM2_1(xrd):
    """ Calculates the vertical pressure differential
    
        input:
        ------
        
            xrd    : an xarray CMIP6 dataset
            
        output:
        -------
        
            dp     : the vertical pressure differential, centered
                     on the model mid-levels
                     [Pa]
                     
    
        Model: CNRM-ESM2-1
        Formula: p = ap + b*ps
    """
    
    # p = a + b*ps
    a = xrd['ap_bnds']
    b = xrd['b_bnds']
    ps = xrd['ps']
    
    # this is a kludge to deal with the fact that this model has bad values
    # for bnds = 1 for both ap and b; this calculates the bnds = 1 values
    # as the average of the mid-level values
    a.isel(bnds = 1).values[:-1] = (xrd['ap'].values[1:] + xrd['ap'].values[:-1])/2
    b.isel(bnds = 1).values[:-1] = (xrd['b'].values[1:] + xrd['b'].values[:-1])/2
    a.isel(bnds = 1).values[-1] = a.isel(bnds = 1).values[-2]
    b.isel(bnds = 1).values[-1] = b.isel(bnds = 1).values[-2]
    
    a.isel(bnds = 0).values[1:] = (xrd['ap'].values[1:] + xrd['ap'].values[:-1])/2
    b.isel(bnds = 0).values[1:] = (xrd['b'].values[1:] + xrd['b'].values[:-1])/2
    a.isel(bnds = 0).values[0] = 0
    b.isel(bnds = 0).values[0] = 1
    
    da = a.isel(bnds = 1) - a.isel(bnds = 0)
    db = b.isel(bnds = 1) - b.isel(bnds = 0)
    
    dp = da + db*ps
    
    return dp
    
def dpressure_from_GFDL_CM4(xrd):
    """ Calculates the vertical pressure differential
    
        input:
        ------
        
            xrd    : an xarray CMIP6 dataset
            
        output:
        -------
        
            dp     : the vertical pressure differential, centered
                     on the model mid-levels
                     [Pa]
                     
    
        Model: GFDL-CM4
        Formula: p(n,k,j,i) = ap(k) + b(k)*ps(n,j,i)
    """
    
    # p = a*p0 + b*ps
    a = xrd['ap_bnds']
    b = xrd['b_bnds']
    ps = xrd['ps']
    
    da = a.isel(bnds = 1) - a.isel(bnds = 0)
    db = b.isel(bnds = 1) - b.isel(bnds = 0)
    
    dp = da + db*ps
    
    return dp
    
def dpressure_from_GISS_E2_1_G(xrd):
    """ Calculates the vertical pressure differential
    
        input:
        ------
        
            xrd    : an xarray CMIP6 dataset
            
        output:
        -------
        
            dp     : the vertical pressure differential, centered
                     on the model mid-levels
                     [Pa]
                     
    
        Model: GISS-E2-1-G
        Formula: p = a*p0 + b*ps
    """
    
    # p = a*p0 + b*ps
    a = xrd['a_bnds']
    b = xrd['b_bnds']
    p0 = xrd['p0']
    ps = xrd['ps']
    
    da = a.isel(bnds = 1) - a.isel(bnds = 0)
    db = b.isel(bnds = 1) - b.isel(bnds = 0)
    
    dp = da*p0 + db*ps
    
    return dp
    
def dpressure_from_MRI_ESM2_0(xrd):
    """ Calculates the vertical pressure differential
    
        input:
        ------
        
            xrd    : an xarray CMIP6 dataset
            
        output:
        -------
        
            dp     : the vertical pressure differential, centered
                     on the model mid-levels
                     [Pa]
                     
    
        Model: MRI-ESM2-0
        Formula: p = a*p0 + b*ps
    """
    
    # p = a*p0 + b*ps
    a = xrd['a_bnds']
    b = xrd['b_bnds']
    p0 = xrd['p0']
    ps = xrd['ps']
    
    da = a.isel(bnds = 1) - a.isel(bnds = 0)
    db = b.isel(bnds = 1) - b.isel(bnds = 0)
    
    dp = da*p0 + db*ps
    
    return dp
    
def dpressure_from_SAM0_UNICON(xrd):
    """ Calculates the vertical pressure differential
    
        input:
        ------
        
            xrd    : an xarray CMIP6 dataset
            
        output:
        -------
        
            dp     : the vertical pressure differential, centered
                     on the model mid-levels
                     [Pa]
                     
    
        Model: SAM0-UNICON
        Formula: p = a*p0 + b*ps
    """
    
    # p = a*p0 + b*ps
    a = xrd['a_bnds']
    b = xrd['b_bnds']
    p0 = xrd['p0']
    ps = xrd['ps']
    
    da = a.isel(bnds = 1) - a.isel(bnds = 0)
    db = b.isel(bnds = 1) - b.isel(bnds = 0)
    
    dp = da*p0 + db*ps
    
    return dp
    
def dpressure_from_IPSL_CM6A_LR(xrd):
    """ Calculates the vertical pressure differential
    
        input:
        ------
        
            xrd    : an xarray CMIP6 dataset
            
        output:
        -------
        
            dp     : the vertical pressure differential, centered
                     on the model mid-levels
                     [Pa]
                     
    
        Model: IPSL-CM6A-LR
        Formula: p = ap + b*ps (this is a guess based on the provided variables)
    """
    
    # p = a*p0 + b*ps
    a = xrd['ap_bnds']
    b = xrd['b_bnds']
    ps = xrd['ps']
    
    da = a.isel(bnds = 0).diff(dim = 'klevp1')
    db = b.isel(bnds = 0).diff(dim = 'klevp1')
    
    dp = da + db*ps
    
    dp = dp.drop('klevp1').rename(dict(klevp1 = 'presnivs'))
    dp['presnivs'] = xrd['presnivs']
    
    return dp
    
dpressure_calculator = {}
dpressure_calculator['BCC-CSM2-MR'] = dpressure_from_BCC_CSM2_MR
dpressure_calculator['CESM2'] = dpressure_from_CESM2
dpressure_calculator['CNRM-CM6-1'] = dpressure_from_CNRM_CM6_1
dpressure_calculator['CNRM-ESM2-1'] = dpressure_from_CNRM_CM6_1
dpressure_calculator['GFDL-CM4'] = dpressure_from_GFDL_CM4
dpressure_calculator['GISS-E2-1-G'] = dpressure_from_GISS_E2_1_G
dpressure_calculator['MRI-ESM2-0'] = dpressure_from_MRI_ESM2_0
dpressure_calculator['SAM0-UNICON'] = dpressure_from_SAM0_UNICON
dpressure_calculator['IPSL-CM6A-LR'] = dpressure_from_IPSL_CM6A_LR


def get_level_variable_name(xr_dataset, model = None):
    """Gets the dimension name for an IPCC model file
    
        input:
        ------
        
            xr_dataset : (xarray.DataSet) an input dataset with associated
                         coordinate variables
                         
            model      : (str) the name of the model from which 
                         the data came. If not provided, this will be
                         inferred from the source_id attribute of
                         the input dataset
 
                         
        output:
        -------
        
            dim_name, model   : the name of the level dimension, and the model name
            
            
        Raises RuntimeError if the model name isn't in the variable metadata and NotImplementedError if 
        the model isn't in the list of models for which this module can work.
 
    
    """
    if 'source_id' in xr_dataset.attrs:
        model = xr_dataset.attrs['source_id']
    else:
        model = None
        
    if model is None:
        raise RuntimeError("Could not infer model name from the attributes of xr_dataset.  Please explicitly set 'model'.")
    
    # check if we have the information to integrate verticall for this model
    if model not in dpressure_calculator:
        raise NotImplementedError("Vertical integration for model `{}` has not been implemented.".format(model))
        
    # set the summation dimension
    dim_name = "lev"
    if model == "IPSL-CM6A-LR":
        dim_name = "presnivs"
        
    return dim_name, model
 
 

def integrate(xr_dataset,
              model = None,
              variables = None):
    """ Calculates the vertical, mass-weighted integral of `xr_dataset`.
    
    
        input:
        ------
        
            xr_dataset : (xarray.DataSet) an input dataset with associated
                         coordinate variables
            
            model      : (str) the name of the model from which 
                         the data came. If not provided, this will be
                         inferred from the source_id attribute of
                         the input dataset
                         
            variables  : a list of variables to integrate.  If None is given
                         all variables are integrated.
            
        output:
        -------
        
            int_dataset : an xarray.DataArray containing the vertical,
                          mass-weighted integral of `xr_dataset`.
    
    """
    
    
    dim_name, model = get_level_variable_name(xr_dataset, model)
   
    # get the mass-weighting term
    dp = neg_one_over_g*dpressure_calculator[model](xr_dataset)
    
   # ensure that dp has the correct ordering
    dp = dp.transpose('time', dim_name, 'lat', 'lon')
    
    # ensure that dp and the output variable have the same vertical coordinate 
    # this is a kludge to deal with the fact that level information changes for the CESM model
    # for some years
    dp = dp.assign_coords(**{dim_name : xr_dataset[dim_name]})
    
    if variables is None:
        # weight the variable
        weighted_xr_dataset = dp * xr_dataset

        # calculate the integral
        int_xr_dataset = weighted_xr_dataset.sum(dim = dim_name)
    else:
        int_xr_dataset = xr_dataset.drop(variables)
        
        for var in variables:
            weighted_var = dp * xr_dataset[var]
            int_xr_dataset[var] = weighted_var.sum(dim = dim_name)
    
    # return the integrated xr_dataset
    return int_xr_dataset


def safe_multiply(ds1, ds2, var1, var2):
    """Multiplies two 3D IPCC variables from the same dataset, dealing with the possibility that level data are corrupted.
    
        input:
        ------
            ds1, ds2      : xarray.Dataset objects containing the variables to multiply
            
            var1, var2    : the variable names in ds1 and ds2 respectively
            
            
        output:
        
            result : an xarray.DataArray object with the result
    """
    
    # get the level dimension name
    dim_name, model = get_level_variable_name(ds1)
    
    return ds1[var1].assign_coords(**{dim_name : ds2[dim_name]}) * ds2[var2]
    
    
    