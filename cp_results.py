import os, glob, shutil

CCLE_DIR = "/lustre/scratch112/sanger/cgppipe/cttv-rnaseq-am26/ccle-fusions/"
DEST_DIR = "/nfs/team78pc20/cttv-rnaseq-am26/ccle-fusions/"

cp_extentions = ["html", "txt"]

class Main(object):
    def __init__(self):
        for mode in ["internal", "external"]:
            for f in self.copy_mode(mode):
                print f
                self.copy_file(mode, f)

    def copy_file(self, mode, f):
        filename = os.path.basename(f)
        dest = os.path.join(DEST_DIR, mode, filename)
        shutil.copy2(f, dest)

    def copy_mode(self, mode):
        files = []
        for sample in glob.glob(os.path.join(CCLE_DIR, mode, "*")):
            if os.path.isdir(sample):
                for ext in cp_extentions:
                    files.extend(self.get_ext(mode, sample, ext))
        return files

    def get_ext(self, mode, sample, ext):
        return glob.glob(os.path.join(CCLE_DIR, mode, sample, "results", "*.%s"%ext))

def main():
    Main()

if __name__ == "__main__":
    main()
