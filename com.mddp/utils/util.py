from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def getsparksession(appname):
    spark = SparkSession.builder.appName(appname).getOrCreate()
    return spark

# Function to read the input df:
def readfile(spark, filepath, fileFormat):
    #print(filepath)
    #print(fileFormat)
    if(fileFormat.lower()=="csv" or fileFormat.lower()=="txt"):
        df = spark.read.format(fileFormat).option("header","true").load(filepath)
    elif fileFormat.lower()=="json":
        df = spark.read.format(fileFormat).option("multiline", "true").load(filepath)
    else:
        df = spark.read.formatO(fileFormat).load(filepath)
    return df

#Function for writing the output df:
def writeFile(df,output_path, output_format, savemode):
   return df.write.format(output_format).mode(savemode).save(output_path)

#Function for date_validation_check in any df:
def datevalidation(df,current_date):
    current_date = current_date().alias("current_date")
    valid_dates = df.select(col("date"), current_date) \
        .withColumn("is_future_date", date_format(col("date"), "yyyy-MM-dd") > col("current_date")) \
        .drop("current_date")
    invalid_future_dates = df.filter(col("date") > current_date())
    # Display the results
    return valid_dates,invalid_future_dates
#############

def datevalidations(df, datecolumn):

    current_date_str = current_date().cast("string")
    rejected_df = df.filter(col(datecolumn) > current_date_str)
    accepted_df = df.filter(col(datecolumn) <= current_date_str)
    return rejected_df, accepted_df


def datevalidations(df,dates):
    # Add a new column to check if the date is in the future
    df_with_future_dates = df.withColumn("is_future_date", (current_date() < df.dates))

    # Save the future dates aside to a new DataFrame
    future_dates = df_with_future_dates.filter(df_with_future_dates.is_future_date)

    # Show the future dates
    future_dates.show()

    # You can save the future dates to a file or perform any further operations as needed
    # For example, to save as Parquet files:
    future_dates.write.mode("overwrite").parquet("/path/to/save/future_dates.parquet")



def getMetadataRead(row):
    dis={}
    dis["Source_file_path"] = row.Source_file_path
    dis["Source_file_format"] = row.Source_file_format
    dis["Select_col_list"] = row.Select_col_list
    dis["Entityname"] = row.Entityname
    dis["null_checks"] = row.Validated_col_list.null_checks

    dis["ri_check_path"]= row.ri_check_path
    dis["ri_check_format"]= row.ri_check_format
    dis["ri_select_col_list"]=row.ri_select_col_list

    dis["src_pk"]=row.src_pk
    dis["src_ri_fk"]=row.src_ri_fk


    return dis

def selectcolumns(df,selectlist):
    print("selectlist:",selectlist)
    return df.select(selectlist)

'''
def nullcheckvalidation(df,nullchecklist,msgcol):
    print(nullchecklist)
    for nullchekcol in nullchecklist:
      print("nullcheckcol:",nullchekcol)
      #nulldf = df.withColumn("nullvalidation",when(col(nullchekcol).isNull(),nullchekcol+" is null"))
      nulldf = df.withColumn(msgcol, when((col(nullchekcol).isNull() & col(msgcol).isNull()), nullchekcol + ": is null")
                         .when((col(nullchekcol).isNull() & col(msgcol).isNotNull()),concat(col(msgcol), lit("|"), lit(nullchekcol), lit(" value contain null")))
                         .otherwise(col(msgcol)))
    return nulldf
'''

def nullcheckvalidation(df, columnslist, msgcol):
    print("multinullcheck:",columnslist)
    for name in columnslist:
        print("name:", name)
        df= df.withColumn(msgcol, when((col(name).isNull() & col(msgcol).isNull()), name+": is null")
                                    .when((col(name).isNull() & col(msgcol).isNotNull()), concat(col(msgcol),lit("|"),lit(name),lit(" value contain null")))
                                    .otherwise(col(msgcol)))
        #df.show(truncate=False)
    return df

def joinentities(leftdf , leftdfcol,rightdf,rightdfcol,jointype):
  print("in joindf function","leftdf:",leftdf," ,leftdfcol:",leftdfcol)

  joindf = leftdf.join(rightdf,leftdf[leftdfcol] == rightdf[rightdfcol],jointype)
  return joindf

