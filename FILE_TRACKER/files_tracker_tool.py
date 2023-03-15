import warnings
warnings.simplefilter(action='ignore')

import os
import sys
import cx_Oracle
import pandas as pd
from datetime import date
from PyQt5.QtWidgets import QApplication, QWidget, QPushButton, QFileDialog, QLabel, QVBoxLayout, QMessageBox



class LandsTracker(QWidget):
    def __init__(self):
        super().__init__()

        # Create a label to display the selected file paths
        self.path_label_tnt = QLabel('No file selected')
        self.path_label_ats = QLabel('No file selected')

        # Create a button to select the first Excel file
        self.select_button1 = QPushButton('Select a TITAN 009 Report')
        self.select_button1.clicked.connect(lambda: self.select_file(1))

        # Create a button to select the second Excel file
        self.select_button2 = QPushButton('Select an ATS Processing Time Report')
        self.select_button2.clicked.connect(lambda: self.select_file(2))

        # Create a button to read the selected Excel files
        self.read_button = QPushButton('Generate a Tracking Report!')
        self.read_button.clicked.connect(self.execute_program)

        # Create a vertical layout for the widgets
        self.layout = QVBoxLayout()
        self.layout.addWidget(self.select_button1)
        self.layout.addWidget(self.path_label_tnt)
        self.layout.addWidget(self.select_button2)
        self.layout.addWidget(self.path_label_ats)
        self.layout.addWidget(self.read_button)
       # self.layout.addWidget(self.merge_button)

        # Set the layout for the main window
        self.setLayout(self.layout)

        # Initialize the DataFrames
        self.df_tnt = None
        self.df_ats = None
        

    def select_file(self, file_num):
        # Open a file dialog to select the Excel file
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        file_path, _ = QFileDialog.getOpenFileName(self, f'Select Excel File {file_num}', ''
                                                   , 'Excel Files (*.xlsx *.xls)', options=options)

        # Update the label to show the selected file path
        if file_path:
            if file_num == 1:
                self.path_label_tnt.setText(file_path)
            elif file_num == 2:
                self.path_label_ats.setText(file_path)

    

    def execute_program(self):
        print ('\nReading Input files')
        print('...titan report')
        df_tnt =  import_titan (self)
        print('...ats report')
        df_ats= import_ats (self)
        
        print('\nCreating Reports.')
        dfs = []
        
        print('...report 01')
        df_01 = create_rpt_01 (df_tnt,df_ats)
        dfs.append(df_01)
        print (df_01.shape[0])
    
    
    
    
    
    
    
    
    
def import_titan (self):
    """Reads the Titan work ledger report into a df"""
    # Get the selected file paths
    file_path_tnt = self.path_label_tnt.text()
    
     # Read the Excel file as pandas DataFrame
    if file_path_tnt.endswith('.xlsx'):
        df_tnt = pd.read_excel(file_path_tnt,'TITAN_RPT009',
                       converters={'FILE NUMBER':str})
    elif file_path_tnt.endswith('.xls'):
        df_tnt = pd.read_excel(file_path_tnt,'TITAN_RPT009',
                       converters={'FILE NUMBER':str}, engine='xlrd')
    else:
        df_tnt = None
    
    # Display a message box if any of the DataFrames is empty
    if df_tnt is not None and df_tnt.empty:
        QMessageBox.warning(self, 'Empty File', 'The selected Excel file is empty.')
            
    tasks = ['NEW APPLICATION','REPLACEMENT APPLICATION','AMENDMENT','ASSIGNMENT']
    df_tnt = df_tnt.loc[df_tnt['TASK DESCRIPTION'].isin(tasks)]
    
    df_tnt.rename(columns={'COMMENTS': 'TANTALIS COMMENTS'}, inplace=True)
 
    del_col = ['ORG. UNIT','MANAGING AGENCY','BCGS','LEGAL DESCRIPTION',
              'FDISTRICT','ADDRESS LINE 1','ADDRESS LINE 2','ADDRESS LINE 3',
              'CITY','PROVINCE','POSTAL CODE','COUNTRY','STATE','ZIP CODE']
    
    for col in df_tnt:
        if 'DATE' in col:
            df_tnt[col] =  pd.to_datetime(df_tnt[col],
                                   infer_datetime_format=True,
                                   errors = 'coerce').dt.date
        elif 'Unnamed' in col:
            df_tnt.drop(col, axis=1, inplace=True)
        
        elif col in del_col:
            df_tnt.drop(col, axis=1, inplace=True)
            
        else:
            pass
            
    df_tnt.loc[df_tnt['PURPOSE'] == 'AQUACULTURE', 'DISTRICT OFFICE'] = 'AQUACULTURE'
    df_tnt.loc[df_tnt['DISTRICT OFFICE'] == 'COURTENAY', 'DISTRICT OFFICE'] = 'AQUACULTURE'
    df_tnt['DISTRICT OFFICE'] = df_tnt['DISTRICT OFFICE'].fillna(value='NANAIMO')
    
    
    return df_tnt


def import_ats (self):
    """Reads the ATS report into a df"""
    
    # Get the selected file paths
    file_path_ats = self.path_label_ats.text()
    
     # Read the Excel file as pandas DataFrame
    if file_path_ats.endswith('.xlsx'):
        df_ats = pd.read_excel(file_path_ats)
    elif file_path_ats.endswith('.xls'):
        df_ats = pd.read_excel(file_path_ats, engine='xlrd')
    else:
        df_ats = None
    
    # Display a message box if any of the DataFrames is empty
    if df_ats is not None and df_ats.empty:
        QMessageBox.warning(self, 'Empty File', 'The selected Excel file is empty.')
    
    df_ats['File Number'] = df_ats['File Number'].fillna(0)
    df_ats['File Number'] = df_ats['File Number'].astype(str)
    
    df_ats.rename(columns={'Comments': 'ATS Comments'}, inplace=True)
    
    df_ats.loc[(df_ats['Accepted Date'].isnull()) & 
       (df_ats['Rejected Date'].isnull()) & 
       (df_ats['Submission Review Complete Date'].notnull()),
       'Accepted Date'] = df_ats['Submission Review Complete Date']
    
    for index,row in df_ats.iterrows():
        z_nbr = 7 - len(str(row['File Number']))
        df_ats.loc[index, 'File Number'] = z_nbr * '0' + str(row['File Number'])
        
    for col in df_ats:
        if 'Date' in col:
            df_ats[col] =  pd.to_datetime(df_ats[col],
                                   infer_datetime_format=True,
                                   errors = 'coerce').dt.date
        elif 'Unnamed' in col:
            df_ats.drop(col, axis=1, inplace=True)
        
        else:
            pass
            
    
    return df_ats


def create_rpt_01(df_tnt,df_ats):
    """ Creates Report 01- New Files in FCBC, not accepted"""
    ats_a = df_ats.loc[df_ats['Authorization Status'] == 'Active']
    #active = ats_a['File Number'].to_list()
    
    df_01= ats_a.loc[(ats_a['Received Date'].notnull()) & (ats_a['Accepted Date'].isnull())]
    
     
    df_01['tempo_join_date']= df_01['Accepted Date'].astype('datetime64[Y]')
    df_tnt['tempo_join_date']= df_tnt['CREATED DATE'].astype('datetime64[Y]')
    
    df_01 = pd.merge(df_01, df_tnt, how='left',
                     left_on=['File Number','tempo_join_date'],
                     right_on=['FILE NUMBER','tempo_join_date'])
    
    df_01.sort_values(by=['Received Date'], ascending=False, inplace=True)
    df_01.reset_index(drop = True, inplace = True)

    return df_01


























app = QApplication(sys.argv)

# Create the ExcelReader window
window = LandsTracker()
window.setWindowTitle('Lands Tracker')

# Show the ExcelReader window
window.show()



# Run the Qt event loop
sys.exit(app.exec_())
