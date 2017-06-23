
# 
# we need to remove the "NULL" values inside the CSV files of the LSST dataset
# this script get the CSV files stored in HDFS, change the NULL values to empty value
# and restore the modified file
#

dest=/user/christian.arnault/swift
tmp=/mongo/log

for f in `hdfs dfs -ls ${dest}/Object_* | sed -e 's#.*swift/##'`
do
   echo $f
   hdfs dfs -cat ${dest}/$f | sed -e 's#NULL##g' > ${tmp}/$f

   hdfs dfs -rm ${dest}/$f
   hdfs dfs -put ${tmp}/$f ${dest}/$f

   rm ${tmp}/$f
done 
