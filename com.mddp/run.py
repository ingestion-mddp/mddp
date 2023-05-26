from pyspark.sql.types import Row
from utils.util import *
from pyspark.sql import functions as f

VALIDATE_MESSAGE_COLUMN="Null_validations"

spark = getsparksession("metadata read")
print(spark)

path = "C:\\Users\\Srihari\\Documents\\venusirsw\\PycharmProjects\\pythonProject\\mddp\\resources\\appMetadata.json"

"metadata read"
metadataDf = readfile(spark,path,"json")
metadataDf.printSchema()

#"data processing row process "
for row in metadataDf.collect():
    print("-----------------------------------------------------------------------------------")
    print("row values:",row)
    print("-----------------------------------------------------------------------------------")
    dis=getMetadataRead(row) #COLUMN VALUES SPLIT

    print("-----------------------------------------------------------------------------------")
    print(dis.keys(), dis.values())
    print(dis["Entityname"])#-----------------------------------------------------------------------------------")

#    Dflist=["parent","child"]
#    if(dis["Entityname"]=="parententity"):
    srcDf= readfile(spark,dis["Source_file_path"],dis["Source_file_format"])
    srcDf.show(5)

    # select
    srcselectDf=selectcolumns(srcDf,dis["Select_col_list"])
    srcselectDf.show()

    if "null_checks" in dis.keys():
        if dis["null_checks"] != None and dis["null_checks"] != "":
                tonullcheckdf = srcselectDf.withColumn(VALIDATE_MESSAGE_COLUMN, lit(None).cast("string"))
                nullcheckDf = nullcheckvalidation(tonullcheckdf,dis["null_checks"],VALIDATE_MESSAGE_COLUMN)
    #print("Validated_col_list:",dis["Validated_col_list[null_checks]"])
                nullcheckDf.show(truncate=False)

    #reading ri entity
    riDf = readfile(spark, dis["ri_check_path"], dis["ri_check_format"])
    riDf.show(5)
    selectlriDf=selectcolumns(riDf,dis["ri_select_col_list"])
    selectlriDf.show()

    #join src and ri

    srcridf=joinentities(srcselectDf,dis["src_pk"],selectlriDf,dis["src_ri_fk"],"inner")
    srcridf.show()


