# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC ######Very Important - path: PathOrPaths, schema: Optional[Union[StructType, str]]=None, sep: Optional[str]=None,header: Optional[Union[bool, str]]=None, inferSchema: Optional[Union[bool, str]]=None, 
# MAGIC ######Important - mode: Optional[str]=None, columnNameOfCorruptRecord: Optional[str]=None,  quote: Optional[str]=None, escape: Optional[str]=None, 
# MAGIC Not Important but good to know once - encoding: Optional[str]=None, comment: Optional[str]=None,ignoreLeadingWhiteSpace: Optional[Union[bool, str]]=None, ignoreTrailingWhiteSpace: Optional[Union[bool, str]]=None, nullValue: Optional[str]=None, nanValue: Optional[str]=None, positiveInf: Optional[str]=None, negativeInf: Optional[str]=None, dateFormat: Optional[str]=None, timestampFormat: Optional[str]=None, maxColumns: Optional[Union[int, str]]=None, maxCharsPerColumn: Optional[Union[int, str]]=None, maxMalformedLogPerPartition: Optional[Union[int, str]]=None,   multiLine: Optional[Union[bool, str]]=None, charToEscapeQuoteEscaping: Optional[str]=None, samplingRatio: Optional[Union[float, str]]=None, enforceSchema: Optional[Union[bool, str]]=None, emptyValue: Optional[str]=None, locale: Optional[str]=None, lineSep: Optional[str]=None, pathGlobFilter: Optional[Union[bool, str]]=None, recursiveFileLookup: Optional[Union[bool, str]]=None, modifiedBefore: Optional[Union[bool, str]]=None, modifiedAfter: Optional[Union[bool, str]]=None, unescapedQuoteHandling: Optional[str]=None) -> "DataFrame"

# COMMAND ----------

# We have so many options to read the files. Let's explore important main options

# Mode Option

read_df = spark.read.