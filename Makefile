
#        make tar
#        make scp
#        make backup
#
# OPTIONS: no options
#
#######################################################################

#######################################################################
# Set defaults

##Backup addresses
##Standrechner
BACKADR=/home/cschlund/programs/python
##Lenovo laptop 
#BACKADR2=cornelia@connypc:/data/progs/proc
##hc,qc home directory
# BACKADR3=/home/home/cornelia/proc
##Backup file
BACKFIL=visAVHRRv3.0_`date -I`_backup.tgz
#######################################################################


# An additional possibility to backup your data 
SUBS = .git/ #OUT #TMP #STA
FILS = *.py Makefile .gitignore
tar:
	tar cvfz ../$(BACKFIL) $(FILS) $(SUBS)

# Write the backed-up tarball to somewhere
# scp:
# 	scp ../$(BACKFIL) $(BACKADR1)

# Write the backed-up tarball to somewhere
cp:
	cp ../$(BACKFIL) $(BACKADR)

backup: 
	make tar
# 	make scp
	make cp
	rm ../$(BACKFIL)
