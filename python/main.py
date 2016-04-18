from optparse import OptionParser
from pyspark import SparkContext
from dictionaries import D
from geoname_extractor import processDoc
from ProbabilisticER import recordLinkage


if __name__ == "__main__":
    sc = SparkContext(appName="DIG-EntityResolution")

    parser = OptionParser()

    (c_options, args) = parser.parse_args()

    input_path = args[0]
    output_path = args[1]
    prior_dict_file = args[2]
    topk = args[3]
    state_dict_path = args[4]
    all_city_path = args[5]
    city_faerie = args[6]
    state_faerie = args[7]
    all_faerie = args[8]
    tagging_dict_file = args[9]
    ERconfig = args[10]

    input_rdd = sc.textFile(input_path)

    dictc = D(sc, state_dict_path, all_city_path, city_faerie, state_faerie, all_faerie, prior_dict_file,tagging_dict_file)

    d = sc.broadcast(dictc)

    results = input_rdd.map(lambda x:processDoc(x,d)).map(lambda x:recordLinkage(ERconfig,x,topk,d.priorDicts,d.taggingDicts))
