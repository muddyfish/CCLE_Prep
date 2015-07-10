import json, os, functools, glob, shutil

CCLE_DIR = "/lustre/scratch112/sanger/cgppipe/cttv-rnaseq-am26/ccle-fusions/"
#Files that need to be present for the job to be considered 'sucessfully finished'
COMPLETED_FILES = ["logs_star",
                   "logs_tophat",
                   "%s.star.Aligned.out.bam",
                   "%s.star.Chimeric.out.bam",
                   "%s.star-fusion.normals.filtered.txt",
                   "%s.tophat.accepted_hits.bam",
                   "%s.tophatfusion.html",
                   "%s.tophat-fusion.normals.filtered.strand.txt",
                   "%s.tophat.unmapped.bam"]
#Files that are only present if the job is still running or failed
TMP_FILES = ["tmpTophatFusion",
             "tmpStar"]
#The destination for copied results files once finished
DEST_DIR = "/nfs/team78pc20/cttv-rnaseq-am26/ccle-fusions/"
#File extentions that should be copied
cp_extentions = ["html", "txt"]

class Main(object):
    def __init__(self):
        self.sample_names_file = open("sample_names.txt")
        #Hackish way to load the sample names without using eval
        #It's pretty much a json file but with single quotes
        self.sample_names = json.loads(self.sample_names_file.readlines()[-1])
        self.sample_names_file.close()
        self.batch_id = self.get_external()
        self.batch_path = os.path.join(CCLE_DIR, self.batch_id)
        #For each sample, does it meet the 'complete' criteria?
        self.samples_complete = map(functools.partial(self.check_sample_files, COMPLETED_FILES), self.sample_names)
        #For each sample, does it meet the 'failed' criteria?
        self.samples_failed  = map(functools.partial(self.check_sample_files, TMP_FILES), self.sample_names)
        #Cleanup
        if self.batch_id == "internal":
            self.cleanup()
        #Output results
        for i in zip(self.sample_names, self.samples_complete, self.samples_failed):
            print i
        print sum(self.samples_complete), "/", len(self.samples_complete), "samples complete"

    def cleanup(self):
        deleted = 0
        copied = 0
        master = open(os.path.join(CCLE_DIR, "tracking", "cell-lines-tracking-MASTER.txt"), "r+")
        for sample_name, complete in zip(self.sample_names, self.samples_complete):
            if complete:
                copied+=self.copy_results(sample_name)
                self.update_spreadsheet(master, sample_name)
                if self.cleanup_delete(sample_name):
                    deleted += 1
        master.close()
        print "In the cleanup step, %d samples were deleted, %d samples had their spreadsheet data updated and %d files were copied"%(deleted, sum(self.samples_complete), copied)

    def cleanup_delete(self, sample_name):
        data_dir = os.path.join(self.batch_path, sample_name, "data")
        for f in glob.glob(os.path.join(data_dir, "*")):
            os.remove(f)
        else: return False
        return True

    def update_spreadsheet(self, master, sample_name):
        master.seek(0)
        while 1:
            line = master.readline()
            if line.split("\t")[0] == sample_name:
                master.seek(-5, 1)
                master.write("DONE")
            if line == "": break

    def copy_results(self, sample_name):
        files = []
        for ext in cp_extentions:
            files.extend(glob.glob(os.path.join(self.batch_path, sample_name, "results", "*.%s"%ext)))
        for f in files:
            shutil.copy2(f, os.path.join(DEST_DIR, os.path.basename(f)))
        return len(files)

    def check_sample_files(self, file_list, sample):
        results_path = os.path.join(self.batch_path, sample, "results")
        for req_file in file_list:
            try:
                exists = os.path.exists(os.path.join(results_path, req_file%sample))
            except TypeError: #If the path doesn't have a %s in it
                exists = os.path.exists(os.path.join(results_path, req_file))
            if not exists:
                #print sample, os.path.join(results_path, req_file), "NOT FOUND"
                return False
        return True

    def get_external(self): 
        #Does the sample name contain the external signature?
        #NOTE: Only checks the first sample name, samples must be all external or all internal
        if self.sample_names[0][-9:] == "_RNA_TCGA":
            return "external"
        return "internal"

def main():
    Main()

if __name__ == "__main__":
    main()
