from pyspark import SparkContext, SparkConf
from optparse import OptionParser
from digSparkUtil.fileUtil import FileUtil
import json


def filter_population(x, pop):
    # print json.dumps(x)

    if 'populationOfArea' in x:
        print 'I have:' + str(x['populationOfArea']) + ' population'
        if int(x['populationOfArea']) >= pop:
            return True
    return False

if __name__ == "__main__":
    sc = SparkContext(appName="DIG-FILTER-BY-POPULATION")

    parser = OptionParser()

    parser.add_option("-r", "--separator", dest="separator", type="string",
                      help="field separator", default="\t")

    (c_options, args) = parser.parse_args()
    print "Got options:", c_options

    inputFilename = args[0]
    outputFilename = args[1]
    population = int(args[2])
    print 'population:', population
    fileUtil = FileUtil(sc)

    input_rdd = fileUtil.load_file(inputFilename, "text", "json")
    output_rdd = input_rdd.filter(lambda x: filter_population(x[1], population))
    output_rdd = output_rdd.coalesce(10)
    fileUtil.save_file(output_rdd, outputFilename, 'text', 'json')