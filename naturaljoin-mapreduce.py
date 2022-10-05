from mrjob.job import MRJob
import re

class MRWordCount(MRJob):

    def mapper(self, _, line): # This function is called once per line
        
        (key, value1, value2) = re.split(",",line) # Split each word
        if (key == "R"): # R(a,b)
            yield(value2, (value1, "R")) # b,(a,R)
        else:  # S(b,c)
            yield (value1, (value2, "S")) # b,(c,S)

    def reducer(self, b, values): # key is b
        valuesList = list(values)
        valuesA = [a for (a, m) in valuesList if m == "R"] # a,R
        valuesC = [c for (c, m) in valuesList if m == "S"] # c,S
        for a in valuesA: # for each a
            for c in valuesC: # for each c
                yield (a, (b, c))

if __name__ == '__main__':
     MRWordCount.run()