from pyspark import SparkContext, SparkConf, StorageLevel
from workflow import Workflow
from py4j.java_gateway import java_import
from optparse import OptionParser
from digSparkUtil.fileUtil import FileUtil
from basicMerger import EntityMerger

# Executed as:
#
# ./makeSpark.sh
#
#
# spark-submit --master local[*]    --executor-memory=8g     --driver-memory=8g \
# --jars /Users/amandeep/Github/Web-Karma/karma-mr/target/karma-mr-0.0.1-SNAPSHOT-shaded.jar  \
# --archives /Users/amandeep/Github/dig-alignment/versions/3.0/karma.zip     \
# --py-files lib/python-lib.zip  \
# --driver-class-path /Users/amandeep/Github/Web-Karma/karma-spark/target/karma-spark-0.0.1-SNAPSHOT-shaded.jar   \
# karmaWorkflowGeo.py /tmp/geonames/input  /tmp/geonames/karma-out

if __name__ == "__main__":
    sc = SparkContext(appName="DIG-GEONAMES")

    parser = OptionParser()

    parser.add_option("-r", "--separator", dest="separator", type="string",
                      help="field separator", default="\t")

    (c_options, args) = parser.parse_args()
    print "Got options:", c_options

    java_import(sc._jvm, "edu.isi.karma")
    inputFilename = args[0]
    outputFilename = args[1]
    city_context = "https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/development/versions/3.0/datasets/geonames/allCountries/city_context.json"
    state_context = "https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/development/versions/3.0/datasets/geonames/allCountries/state_context.json"
    country_context = "https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/development/versions/3.0/datasets/geonames/allCountries/country_context.json"

    fileUtil = FileUtil(sc)
    workflow = Workflow(sc)

    # 1. Read the input
    inputRDD = workflow.batch_read_csv(inputFilename)
    inputRDD_partitioned = inputRDD.partitionBy(100)

    #2. Apply the karma Model
    cityRDD1 = workflow.run_karma(inputRDD_partitioned,
                                   "https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/development/versions/3.0/datasets/geonames/allCountries/city_model.ttl",
                                   "http://dig.isi.edu/geonames",
                                   "http://schema.org/City1",
                                   city_context,
                                   data_type="csv",
                                   additional_settings={"karma.input.delimiter":"\t", "rdf.generation.disable.nesting":"false"})
    stateRDD1 = workflow.run_karma(inputRDD_partitioned,
                                   "https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/development/versions/3.0/datasets/geonames/allCountries/state_model.ttl",
                                   "http://dig.isi.edu/geonames",
                                   "http://schema.org/State1",
                                   state_context,
                                   data_type="csv",
                                   additional_settings={"karma.input.delimiter":"\t", "rdf.generation.disable.nesting":"false"})
    countryRDD1 = workflow.run_karma(inputRDD_partitioned,
                                   "https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/development/versions/3.0/datasets/geonames/allCountries/country_model.ttl",
                                   "http://dig.isi.edu/geonames",
                                   "http://schema.org/Country1",
                                   country_context,
                                   data_type="csv",
                                   additional_settings={"karma.input.delimiter":"\t", "rdf.generation.disable.nesting":"false"})

    # Apply the context
    cityRDD = workflow.apply_context(cityRDD1, city_context)
    stateRDD = workflow.apply_context(stateRDD1, state_context)
    countryRDD = workflow.apply_context(countryRDD1, country_context)

    fileUtil.save_file(countryRDD, outputFilename+"_Country", "text", "json")
    fileUtil.save_file(cityRDD, outputFilename+"_City", "text", "json")
    fileUtil.save_file(stateRDD, outputFilename+"_State", "text", "json")


    mergeRDD1 = EntityMerger.merge_rdds(stateRDD, "address.addressCountry", countryRDD,100)
    fileUtil.save_file(mergeRDD1, outputFilename+"_State_Country", "text", "json")

    mergeRDD2 = EntityMerger.merge_rdds(cityRDD, "address.addressRegion", mergeRDD1,100)

    #3. Save the output
    fileUtil.save_file(mergeRDD2, outputFilename, "text", "json")