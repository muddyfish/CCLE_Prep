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
        #Load the last line of the sample names as json.
        #The rest of it is for archive purposes only
        self.sample_names_file = open("sample_names.txt")
        self.sample_names = json.loads(self.sample_names_file.readlines()[-1])
        self.sample_names_file.close()
        #Decide if the samples are internal or external
        self.sample_batches = self.get_external()
        #Where are the samples located
        #For each sample, does it meet the 'complete' criteria?
        self.samples_complete = map(functools.partial(self.check_sample_files, COMPLETED_FILES), self.sample_names)
        #For each sample, does it meet the 'failed' criteria?
        self.samples_failed  = map(functools.partial(self.check_sample_files, TMP_FILES), self.sample_names)
        #Cleanup
        self.cleanup()
        #Output results
        for i in zip(self.sample_names, self.samples_complete, self.samples_failed):
            if i[1] == False: print i
        print sum(self.samples_complete), "/", len(self.samples_complete), "samples complete"

    def cleanup(self):
        samples_deleted = 0
        samples_copied = 0
        #Master tracking file that contains the sample list
        master = open(os.path.join(CCLE_DIR, "tracking", "cell-lines-tracking-MASTER.txt"), "r+")
        for sample_name, sample_complete in zip(self.sample_names, self.samples_complete):
            if sample_complete:
                samples_copied += self.copy_results(sample_name)
                self.update_spreadsheet(master, sample_name)
                if self.sample_batches[sample_name] == "internal" and self.cleanup_delete(sample_name):
                    samples_deleted += 1
        master.close()
        print "In the cleanup step, " \
              "%d samples were deleted, " \
              "%d samples had their spreadsheet data updated and " \
              "%d files were copied" \
              %(samples_deleted, sum(self.samples_complete), samples_copied)

    def cleanup_delete(self, sample_name):
        #Find the correct data directory
        data_dir = os.path.join(self.sample_batches[sample_name], sample_name, "data")
        deleted = False
        for f in glob.glob(os.path.join(data_dir, "*")):
            os.remove(f)
            deleted = True
        #Return if files were deleted
        return deleted

    def update_spreadsheet(self, master, sample_name):
        #Goto the beginning of master
        master.seek(0)
        while 1:
            line = master.readline()
            #If the line coresponds to the current sample
            if line.split("\t")[0] == sample_name:
                #Seek backwards
                master.seek(-5, 1)
                #Replace NEXT with DONE
                master.write("DONE")
            #If EOF, end
            if line == "": break

    def copy_results(self, sample_name):
        files = []
        for ext in cp_extentions:
            #Glob expression for finding files in the results directory that have the correct extention
            expression = os.path.join(self.sample_batches[sample_name], sample_name, "results", "*.%s"%ext)
            #Add the matching files to the list
            files.extend(glob.glob(expression))
        #Copy them
        copied = 0
        for f in files:
            dest = os.path.join(DEST_DIR, os.path.basename(f))
            if not os.path.isfile(dest):
                shutil.copy2(f, dest)
                copied+=1
        #Return the number of files copied
        return copied

    def check_sample_files(self, file_list, sample):
        results_path = os.path.join(self.sample_batches[sample], sample, "results")
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
        sample_batches = {}
        int_path = os.path.join(CCLE_DIR, "internal")
        ext_path = os.path.join(CCLE_DIR, "external")
        for sample in self.sample_names:
            sample_batches[sample] = {True: ext_path, False: int_path}[sample[-9:] == "_RNA_TCGA"]
        return sample_batches

def main():
    Main()

if __name__ == "__main__":
    main()
