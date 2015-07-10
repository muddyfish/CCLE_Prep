import os, datetime, json, sys

CCLE_DIR = "/lustre/scratch112/sanger/cgppipe/cttv-rnaseq-am26/ccle-fusions/"
TRACKING_ID = "tracking"
TRACKING_MASTER_FILE = "cell-lines-tracking-MASTER.txt"
TRACKING_MASTER_FILE_BACKUP = "cell-lines-tracking-MASTER.txt.backup"

BATCH_ID = "internal"
assert(BATCH_ID in ["internal", "external"])
NO_SAMPLES = 25

class Main(object):
    def __init__(self):
        """Setup the RNA-Seq fusion pipeline
Outputs a bpipe command that will start the pipeline"""
        cwd = os.getcwd()
        self.batch_path = os.path.join(CCLE_DIR, BATCH_ID)
        self.master_filename = os.path.join(CCLE_DIR, TRACKING_ID, TRACKING_MASTER_FILE)
        self.master_file = open(self.master_filename, "r") 
        self.db = self.read_master()
        self.master_file.close()
        #Write a backup master file
        self.write_backup() 
        #Choose the samples that should be processed next
        self.sample_names = self.choose_samples()
        #Overwrite the master file with the updated verison
        self.master_file = open(self.master_filename, "w")
        self.master_file.write(self.write_master())
        self.master_file.close()
        #Prepare pipeline file
        run_filename = self.prepare_pipeline_file()
        #Prepare bpipe command
        #JOB NAME, STDOUT PATH, STDERR PATH
        #Can be taken from sys.argv and if 1 arg supplied, used for all 3
        #If 2 supplied, used for job name and STDOUT and STDERR will be the same
        #If 3 supplied, will be unique
        job_names = ["NEXT", "NEXT", "NEXT"]
        for i in range(3):
            if len(sys.argv) > i+1:
                job_names[i:] = [sys.argv[i+1]]*(3-i)
        bpipe_command = self.prepare_bpipe_command(run_filename, job_names)
        #Write sample names to last line in file
        os.chdir(cwd)
        sample_names_file = open("sample_names.txt", "a")
        sample_names_file.write(json.dumps(self.sample_names)+"\n")
        sample_names_file.close()
        #Output results
        print self.sample_names
        print bpipe_command
        

    def choose_samples(self):
        "Choose the next N samples to work on. They will have the NEXT tag appended to them"
        samples = filter(lambda sample:
                             sample[1] == BATCH_ID and 
                             sample[2] == "Y" and
                             sample[3] == "",
                         self.db)[:NO_SAMPLES]
        #Update the status field, as only shallow copies, will update self.db
        for sample in samples:
            sample[3] = "NEXT"
        #Return the sample name of all chosen samples
        return [sample[0] for sample in samples]

    def prepare_pipeline_file(self):
        "Prepare the pipeline"
        template_pipeline_file = open(BATCH_ID+".txt")
        template = template_pipeline_file.read()
        template_pipeline_file.close()
        #Replacing spaces might be nessasary
        sample_names = str(self.sample_names)
        os.chdir(self.batch_path)
        #Get the filename to be used for the run file using today's date
        filename = datetime.date.today().strftime("ccle_"+ BATCH_ID[:3] +"_%d%m%Y.run")
        pipeline_file = open(filename, "w")
        pipeline_file.write(template%sample_names)
        pipeline_file.close()
        return filename

    def prepare_bpipe_command(self, run_filename, job_names):
        bpipe_command = "bsub -J {0} -o {1}.o -e {2}.e bpipe run -r {path}".format(*job_names, path = os.path.join(self.batch_path, run_filename))
        return bpipe_command

    def read_master(self):
        "Read the tab seperated tracker file"
        self.master_file.seek(0)
        return [ln[:-1].split("\t") for ln in self.master_file.readlines()]
    
    def write_master(self):
        "The reverse of read_master, writes a db to a tracker string"
        return "\n".join(["\t".join(ln) for ln in self.db])+"\n"

    def write_backup(self):
        "Write the current contents of the tracker to the master backup file"
        self.master_backup = open(os.path.join(CCLE_DIR, TRACKING_ID, TRACKING_MASTER_FILE_BACKUP), "w")

        #self.master_file.seek(0) #Check is identical to orig
        #assert(self.master_file.read()==self.write_master())

        self.master_backup.write(self.write_master())
        self.master_backup.close()

def main():
    Main()

if __name__ == "__main__":
    main()
