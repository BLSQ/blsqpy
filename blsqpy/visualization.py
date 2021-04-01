"""
"""
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


class Visualize(object):
    """
    Support functions for data treatment
    
    ----------
    Parameters
    ----------
        dhis: 
            blspy dhis2 object. 
            
    ----------
    Attributes
    ----------
        dhis: 
            blspy dhis2 object. 
            
    -------
    Methods
    -------
    
    build_de_cc_table(self):
        Builds a table in which category combos are linked to data elements
        

    ________________________________________________________________________
    __________________________________________________________________________

    Attributes(Internal)
    ----------     
        
    Methods (Internal)
    ----------
    _get_organisationLevel_labels(self):
        SQL query to get names of OU levels on the health pyramid
        
    _merger_handler(df,column_labeling_dict)
        Support function for the uid labeling
    
    """

    def __init__(self, dhis2):
        """
        Creates a "support" instance generating its attributes.
        """
        self.dhis2 = dhis2

    @staticmethod
    def general_province_plot(df,col_axis_x,col_axis_y,col_wrap,gtype,ylimit=None):
        if ylimit:
            g = sns.FacetGrid(df, col=col_wrap,col_wrap=5,sharex=True,ylim=(0,ylimit))
        else:
            g = sns.FacetGrid(df, col=col_wrap,col_wrap=5,sharex=True)
        if gtype=='month':
            g.map(plt.plot, col_axis_x, col_axis_y)
        else:
            g.map(plt.plot, col_axis_x, col_axis_y,marker="o")
        g.fig.set_size_inches(14,9)
        for ax in g.axes.flat:
            labels = ax.get_xticklabels() # get x labels
            for i,l in enumerate(labels):
                if(i%2 == 0): labels[i] = '' # skip even labels
            ax.set_xticklabels(labels, rotation=90) # set new labels
    
        plt.show()

    @staticmethod
    def general_one_province_plot(df,col_axis_x,col_axis_y,col_wrap,gtype):
        g = sns.FacetGrid(df, hue=col_wrap)
        if gtype=='month':
            g.map(plt.plot, col_axis_x, col_axis_y)
        else:
            g.map(plt.plot, col_axis_x, col_axis_y,marker="o")
        g.fig.set_size_inches(18,6)

    @staticmethod
    def general_pct_change_province_plot(df,col_axis_x,col_axis_y,col_wrap,ylimit,gtype):
        df['pct_incremental']=df[col_axis_y].pct_change()*100
        g = sns.FacetGrid(df, col=col_wrap,col_wrap=5,ylim=(-ylimit,ylimit) )
        if gtype=='month':
            g.map(plt.plot, col_axis_x, 'pct_incremental')
        else:
            g.map(plt.plot, col_axis_x, 'pct_incremental',marker="o")
        g.map(plt.axhline, y=0, ls='--', c='red')
        g.fig.set_size_inches(14,9)
        for ax in g.axes.flat:
            labels = ax.get_xticklabels() # get x labels
            for i,l in enumerate(labels):
                if(i%2 == 0): labels[i] = '' # skip even labels
            ax.set_xticklabels(labels, rotation=90) # set new labels
        plt.show()
