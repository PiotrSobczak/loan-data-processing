import json

import findspark
findspark.init()

from pyspark.sql.types import *


def load_schema(schema_path):
    return StructType.fromJson(json.loads(open(schema_path).read()))


def generate_schema_from_json(spark_session, path):
    return spark_session.read.json(path).schema


def map_spark_types_to_hive(spark_type):
    if spark_type == 'StringType':
        return "string"
    elif spark_type == "LongType":
        return "bigint"
    elif spark_type == "DoubleType":
        return "decimal"
    else:
        raise Exception("Unknown datatype {}.".format(spark_type))


def generate_hive_table_definition(name, schema):
    create_statement_str = "CREATE TABLE {} {}"
    tuples = ["{} {}".format(field.name, map_spark_types_to_hive(str(field.dataType))) for field in schema.fields]
    return create_statement_str.format(name, "({})".format(",".join(tuples)))
