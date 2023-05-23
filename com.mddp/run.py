from pyspark.sql.types import Row
from utils.util import *

VALIDATE_MESSAGE_COLUMN="Null_validations"

spark = getsparksession("metadata read")
print(spark)

path = "C:\\Users\\Srihari\\Documents\\venusirsw\\PycharmProjects\\pythonProject\\MetaProject\\resources\\appMetadata.json"

"metadata read"
metadataDf = readfile(spark,path,"json")
#metadataDf.printSchema()

#"data processing row process "
for row in metadataDf.collect():
    print("-----------------------------------------------------------------------------------")
    print("row:",row)
    print("-----------------------------------------------------------------------------------")
    dis=getMetadataRead(row) #COLUMN VALUES SPLIT

    print("-----------------------------------------------------------------------------------")
    print(dis.keys(), dis.values())
    print("-----------------------------------------------------------------------------------")
    srcDf = readfile(spark,dis["Source_file_path"],dis["Source_file_format"])
    srcDf.show(5)

    # select
    selectDf=selectcolumns(srcDf,dis["Select_col_list"])
    selectDf.show()

    if "null_checks" in dis.keys():
        if dis["null_checks"] != None and dis["null_checks"] != "":
                tonullcheckdf = selectDf.withColumn(VALIDATE_MESSAGE_COLUMN, lit(None).cast("string"))
                nullcheckDf = nullcheckvalidation(tonullcheckdf,dis["null_checks"],VALIDATE_MESSAGE_COLUMN)
    #print("Validated_col_list:",dis["Validated_col_list[null_checks]"])
                nullcheckDf.show(truncate=False)








