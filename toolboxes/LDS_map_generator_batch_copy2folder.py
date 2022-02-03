import os
import shutil

in_dir = r'\\...'
out_dir = r'\\sfp.idir.bcgov\S164\S63087\Share\Crown Land and Resources\!Land Tenure Files (12800-20)'

for filename in os.listdir(in_dir):
    if filename.endswith('.pdf'):
          src = os.path.join(in_dir, filename)
          tenure_nbr = filename[-11:-4]
          dst = os.path.join(out_dir, str(tenure_nbr),'TENURE  DOCUMENTS')
          print (dst)

          if os.path.isdir(dst):
                print ('Copying {}'.format (str(tenure_nbr)))
                shutil.copy(src, dst)

          else:
            print ('Output directory not found. {} was NOT copied'.format (str(tenure_nbr)))
