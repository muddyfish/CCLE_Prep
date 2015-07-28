import os, glob, json
path = "/lustre/scratch112/sanger/cgppipe/cttv-rnaseq-am26/ccle-fusions/"
batches =["internal", "external"]
samples = []
for batch in batches:
    for sample in glob.glob(os.path.join(path, batch, "*")):
        if os.path.isdir(sample):
            samples.append(os.path.basename(sample))
#print samples
failed = {"internal": [], "external": []}
tsv_file = open(os.path.join(path, "tracking", "cell-lines-tracking-MASTER.txt"))
for line in tsv_file.readlines():
    line = line.rstrip()
    splitted = line.split("\t")
    if splitted[0] not in samples and len(splitted) == 4 and splitted[3] == "NEXT":
        failed[splitted[1]].append(splitted[0])
tsv_file.close()
print failed
print sum(map(len, failed.values()))
