#!/usr/bin/env python

import json
import remus



data = json.loads("""
{"http://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/laml/cgcc/jhu-usc.edu/humanmethylation27/methylation/jhu-usc.edu_LAML.HumanMethylation27.Level_3.1.0.0.tar.gz":{"addedDate":"06-11-2010","platform":"http://tcga-data.nci.nih.gov/tcgadccws/GetXML?query=Platform&Archive[@id=2464]&roleName=platform","cacheURL":"http://localhost:8080/WebCache/e261b152-eaa1-4439-af04-4525b8073d06/5a87b862db573dcbfe735de7bcd344ff43d9487b","isLatest":"1","archiveType":"Level_3","serialIndex":"1","disease":"http://tcga-data.nci.nih.gov/tcgadccws/GetXML?query=Disease&Archive[@id=2464]&roleName=disease","id":"2464","deployStatus":"Available","center":"http://tcga-data.nci.nih.gov/tcgadccws/GetXML?query=Center&Archive[@id=2464]&roleName=center","revision":"0","platformName":"HumanMethylation27","bcrBiospecimenBarcodeCollection":"http://tcga-data.nci.nih.gov/tcgadccws/GetXML?query=BiospecimenBarcode&Archive[@id=2464]&roleName=bcrBiospecimenBarcodeCollection","name":"jhu-usc.edu_LAML.HumanMethylation27.Level_3.1.0.0","baseName":"jhu-usc.edu_LAML_HumanMethylation27","fileCollection":"http://tcga-data.nci.nih.gov/tcgadccws/GetXML?query=FileInfo&Archive[@id=2464]&roleName=fileCollection","deployLocation":"/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/laml/cgcc/jhu-usc.edu/humanmethylation27/methylation/jhu-usc.edu_LAML.HumanMethylation27.Level_3.1.0.0.tar.gz"}}
""")


remus.init("http://localhost:16016")

for key in data:
	print key, data[key]
	instance = remus.submit( "/methylation27:pipelineStart", key, data[key] )
	print instance
