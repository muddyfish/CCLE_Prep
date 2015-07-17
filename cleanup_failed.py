import json, os, functools, glob, datetime, shutil

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

class Main(object):
    def __init__(self):
        #Load the last line of the sample names as json.
        #The rest of it is for archive purposes only
        self.sample_names_file = open("sample_names.txt")
        self.sample_names = json.loads(self.sample_names_file.readlines()[-1])
        self.sample_names_file.close()
        #Decide if the samples are internal or external
        self.batch_id = self.get_external()
        #Where are the samples located
        self.batch_path = os.path.join(CCLE_DIR, self.batch_id)
        #For each sample, does it meet the 'complete' criteria?
        self.samples_complete = map(functools.partial(self.check_sample_files, COMPLETED_FILES), self.sample_names)
        failed = []
        for sample in zip(self.sample_names, self.samples_complete):
            if not sample[1]: failed.append(sample[0])
        self.cleanup(failed)
        self.retry(failed)
        print sum(self.samples_complete), "/", len(self.samples_complete), "samples complete"

    def cleanup(self, samples):
        for sample in samples:
            path = os.path.join(self.batch_path, sample)
            if raw_input("Delete %s? "%path):
                try:
                    shutil.rmtree(path)
                except OSError:
                    print "Already deleted"

    def retry(self, failed):
        run_filename = self.prepare_pipeline_file(failed)
        bpipe_command = "bsub -J RETRY -o RETRY.o -e RETRY.e bpipe run -r {path}".format(path = os.path.join(self.batch_path, run_filename))
        print bpipe_command

    def prepare_pipeline_file(self, sample_names):
        "Prepare the pipeline"
        template_pipeline_file = open(self.batch_id+".txt")
        template = template_pipeline_file.read()
        template_pipeline_file.close()
        #Replacing spaces might be nessasary
        sample_names = str(sample_names)
        os.chdir(self.batch_path)
        #Get the filename to be used for the run file using today's date
        filename = datetime.date.today().strftime("ccle_"+ self.batch_id[:3] +"_%d%m%Y.run")
        pipeline_file = open(filename, "w")
        pipeline_file.write(template%sample_names)
        pipeline_file.close()
        return filename

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
