import random

# Executed as:
#
# ./makeSpark.sh
#
#
# rm -rf karma-out; spark-submit \
#       --archives karma.zip \
#       --py-files lib/python-lib.zip \
#       --driver-class-path ~/Documents/Web-Karma/karma-spark/target/karma-spark-0.0.1-SNAPSHOT-shaded.jar \
#       karmaWorkflowCSV.py allCountries_esc_gh_1000.tsv karma-out

if __name__ == "__main__":
    sc = SparkContext(appName="TEST")

    java_import(sc._jvm, "edu.isi.karma")
    inputFilename = argv[1]
    outputFilename = argv[2]
    contextUrlcity = "https://raw.githubusercontent.com/usc-isi-i2/dig-geonames/master/city_context.json"
    contextUrlstate = "https://raw.githubusercontent.com/usc-isi-i2/dig-geonames/master/state_context.json"
    contextUrlcountry = "https://raw.githubusercontent.com/usc-isi-i2/dig-geonames/master/country_context.json"

    fileUtil = FileUtil(sc)
    workflow = Workflow(sc)

    #1. Read the input
    inputRDD = workflow.batch_read_csv(inputFilename)
    inputRDD_partitioned = inputRDD.partitionBy(100)
    #2. Apply the karma Model
    cityRDD1 = workflow.run_karma(inputRDD_partitioned,
                                   "https://raw.githubusercontent.com/usc-isi-i2/dig-geonames/master/city_model.ttl",
                                   "http://dig.isi.edu/geonames",
                                   "http://schema.org/City1",
                                   contextUrlcity,
                                   data_type="csv",
                                   additional_settings={"karma.input.delimiter":"\t", "rdf.generation.disable.nesting":"false"})
    stateRDD1 = workflow.run_karma(inputRDD_partitioned,
                                   "https://raw.githubusercontent.com/usc-isi-i2/dig-geonames/master/state_model.ttl",
                                   "http://dig.isi.edu/geonames",
                                   "http://schema.org/State1",
                                   contextUrlstate,
                                   data_type="csv",
                                   additional_settings={"karma.input.delimiter":"\t", "rdf.generation.disable.nesting":"false"})
    countryRDD1 = workflow.run_karma(inputRDD_partitioned,
                                   "https://raw.githubusercontent.com/usc-isi-i2/dig-geonames/master/country_model.ttl",
                                   "http://dig.isi.edu/geonames",
                                   "http://schema.org/Country1",
                                   contextUrlcountry,
                                   data_type="csv",
                                   additional_settings={"karma.input.delimiter":"\t", "rdf.generation.disable.nesting":"false"})

    # Apply the context
    context1 = workflow.read_json_file(contextUrlcity)
    cityRDD = workflow.apply_context(cityRDD1, context1, contextUrlcity)
    context2 = workflow.read_json_file(contextUrlstate)
    stateRDD = workflow.apply_context(stateRDD1, context2, contextUrlstate)
    context3 = workflow.read_json_file(contextUrlcountry)
    countryRDD = workflow.apply_context(countryRDD1, context3, contextUrlcountry)
    fileUtil.save_file(countryRDD, outputFilename+"_Country", "text", "json")
    #fileUtil.save_file(cityRDD, outputFilename+"_City", "text", "json")
    fileUtil.save_file(stateRDD, outputFilename+"_State", "text", "json")
    stateRDD_partitioned = stateRDD.partitionBy(100)
    cityRDD_partitioned = cityRDD.partitionBy(100)
    mergeRDD1 = EntityMerger.merge_rdds(stateRDD_partitioned, "address.addressCountry", countryRDD,1)
    fileUtil.save_file(mergeRDD1, outputFilename+"_State_Country", "text", "json")
    mergeRDD2 = EntityMerger.merge_rdds(cityRDD_partitioned, "address.addressRegion", mergeRDD1,1)

    # placeRDD = outputRDDcontext.filter(lambda x: x[1]["a"] == "Place")
    # admRDD = outputRDDcontext.filter(lambda x: x[1]["a"] == "AdaministrativeArea")
    #3. Save the output
    fileUtil.save_file(mergeRDD2, outputFilename, "text", "json")