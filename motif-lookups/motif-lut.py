import re
import pickle
#This is where all of these transcription factors come from
#http://jaspar.binf.ku.dk/html/DOWNLOAD/ARCHIVE/JASPAR2010/JASPAR_CORE/pfms/pfms_all.txt

# then cat pfms_all.txt | grep ">" > jaspar_tf_map.txt 

tf_map_file = 'jaspar_tf_map.txt' 
with open(tf_map_file) as f:
    lines = f.readlines()

lut_jaspar_motifs_by_tf = {} 
lut_tfs_by_jaspar_motifs = {}

for line in lines:
  print("line: " + line)
  matchObj = re.search(r'>(M.+)\s(.+)', line)
  motif_value = matchObj.groups(1)[0]
  tf = matchObj.groups(1)[1]   
  print("motif: " + motif_value + " tf:" + tf)
  lut_tfs_by_jaspar_motifs[motif_value] = tf
  lut_jaspar_motifs_by_tf[tf] = motif_value
  #lookup works both ways!


def save_obj(obj, name ):
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_obj(name ):
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)
  
#save_obj(lut_jaspar_motifs_by_tf, 
#         'lut_jaspar_motifs_by_tf')
#
#save_obj(lut_tfs_by_jaspar_motifs,
#         'lut_tfs_by_jaspar_motif')
#


happy = load_obj('lut_tfs_by_jaspar_motif')
print("happy? " + str(type(happy)))
slappy = load_obj('lut_jaspar_motifs_by_tf')
