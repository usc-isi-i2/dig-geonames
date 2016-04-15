from optparse import OptionParser
import codecs

if __name__ == "__main__":

    parser = OptionParser()

    (c_options, args) = parser.parse_args()

    input_path = args[0]
    output_path = args[1]

    f = codecs.open(input_path, 'r', 'utf-8')
    o = codecs.open(output_path, 'w', 'utf-8')

    lines = f.readlines()
    for line in lines:
        values = line.split('\t')
        if values[2] == 'en' and values[5] == '1'   :
            o.write(line)
            # o.write('\n')

    o.close()
    f.close()