"""Copies all finished results files from luster to rhe backup nfs directory"""
import os, glob, shutil
#Input directory
CCLE_DIR = "/lustre/scratch112/sanger/cgppipe/cttv-rnaseq-am26/ccle-fusions/"
#Output directory
DEST_DIR = "/nfs/team78pc20/cttv-rnaseq-am26/ccle-fusions/"
#Files must have the following extentions to be copied
cp_extentions = ["html", "txt"]

class Main(object):
    def __init__(self):
        files = []
        #Copy both internal and external samples
        for mode in ["internal", "external"]:
            #Get the filenames for the sample
            for f in self.copy_mode(mode):
                files.append([mode, f])
        max_len = max([len(f[1]) for f in files])
        for i, f in enumerate(files):
            files[i][1] = files[i][1]+" "*(max_len-len(files[i][1]))
        #Actually copy them
        for i, f in enumerate(files):
            #Pretty print percentage complete
            print "\r%d%%"%((float(i)/len(files))*100), f[1],
            #Copy the file
            self.copy_file(*f)
        print

    def copy_file(self, mode, f):
        filename = os.path.basename(f.rstrip())
        dest = os.path.join(DEST_DIR, mode, filename)
        shutil.copy2(f.rstrip(), dest)

    def copy_mode(self, mode):
        files = []
        #For every sample
        for sample in glob.glob(os.path.join(CCLE_DIR, mode, "*")):
            if os.path.isdir(sample):
                #For each file extention that we want to copy
                for ext in cp_extentions:
                    #Extend the filelist by the files that follow that extention
                    files.extend(self.get_ext(mode, sample, ext))
        return files

    def get_ext(self, mode, sample, ext):
        #Get all the files in the sample with the extention provided
        return glob.glob(os.path.join(CCLE_DIR, mode, sample, "results", "*.%s"%ext))

def main():
    Main()

if __name__ == "__main__":
    main()
