import pandas as pd
import os


def load(
         input_file_list = "/global/u1/t/taobrien/m1517_taobrien/cmip6_hackathon/cmip6_list_20190904.txt",
         cache_file = 'cmip6_list_20190904.pk',
        ):
    """ Loads the database of available CMIP6 data.
    
        input:
        ------
        
            input_file_list : a list of all available CMIP6 files
        
            cache_file : a file containing a cached version of the database, for fast loads
                         If this file exists, reading of input_file_list is skipped.
            
            
        output:
        -------
        
            Returns a pandas dataframe containing information about each available file on disk:
            
            "center", "model", "simulation", "ensemble", "group", "variable", "gn", "version", "base_path" "filename"
            
            
            If cache_file doesn't already exist, this function attempts to create it.
    
    
    """
    
    # TODO: generalize this to different paths

    if not os.path.exists(cache_file):
        names = ["dum", "d1", "d2", "d3", "d4", "d5", "d6", "center", "model", "simulation", "ensemble", "group", "variable", "gn", "version", "filename"]

        # read the list of files
        full_table = pd.read_csv(input_file_list, sep = '/', names = names)
        # remove the dummy column (the preceeding /)
        full_table = full_table.drop(columns = "dum")
        
        # reconstruct the base path
        full_table['base_path'] = full_table.iloc[:,:6].apply(lambda x: '/' + '/'.join(x) + '/', axis = 1)
        
        # extract the file ID
        full_table['file_id'] = full_table.loc[:, "filename"].apply(lambda fn: fn.split('_')[-1].split('.')[0])
        
        # remove parts of the path that precede the CMIP6 dataset
        full_table = full_table.drop(columns = [ 'd{}'.format(i) for i in range(1,7)])

        # Attempt to save the cache file
        if cache_file is not None:
            try:
                full_table.to_pickle(cache_file)
            except:
                pass
    else:
        full_table = pd.read_pickle(cache_file)


    return full_table
    
    
def select_by_dict(cmip6_table, **kwargs):
    """ Search the CMIP6 data table using a dict for matching.
    
    
        input:
        ------
        
            cmip6_table : a pandas dataframe containing information
                          about available CMIP6 files (e.g. returned
                          from `load()`)
            
            **kwargs    : search terms should be given as keyword arguments
        
        output:
        -------
        
            cmip6_table_subset : a subset of the input cmip6_table, containing
                                 only files that match the search
                                 
    """
    # get any keyward arguments
    search_dict = kwargs
    
    # find all rows that match each term
    criteria = [ cmip6_table[key] == item for key, item in search_dict.items() ]
    
    # combine all rows: only select ones that mutual match all search terms
    iselect = criteria[0]
    for i in range(1, len(criteria)):
        iselect = iselect & criteria[i]
        
    # get the subset of the table
    cmip6_table_subset = cmip6_table[iselect].dropna()
    return cmip6_table_subset


def reconstruct_path(cmip6_table):
    """ Reconstruct the full path for all files in a cmip6 data table
    
        input:
        ------
        
            cmip6_table : a pandas dataframe containing information
                          about available CMIP6 files (e.g. returned
                          from `load()`)
            
        output:
        -------
        
            file_series : a pandas.Series object containing a list of file paths
                                 
    """
    return cmip6_table.loc[:,["base_path","center","model","simulation", "ensemble", "group", "variable", "gn", "version", "filename"]].apply(lambda x: '/'.join(x), axis = 1)

